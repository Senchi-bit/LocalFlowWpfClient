using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using LocalFlowWpfClient.Protocol;

namespace LocalFlowWpfClient.Net;

internal readonly record struct TransferProgress(double Percent, string Status);

internal sealed class TransferException(string message) : Exception(message);

internal static class UdpFileSender
{
    public static async Task SendAsync(string filePath, IPEndPoint remote,
        IProgress<TransferProgress>? progress, CancellationToken cancellationToken)
    {
        var file = new FileInfo(filePath);
        if (!file.Exists)
            throw new TransferException("Файл не найден.");
        if (file.Length > ClientSettings.MaxFileSizeBytes)
            throw new TransferException("Файл больше 10 ГиБ.");

        var transferId = Guid.NewGuid();
        using var stream = new FileStream(
            file.FullName,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 64 * 1024,
            FileOptions.RandomAccess);

        using var udp = new UdpClient(new IPEndPoint(IPAddress.Any, 0));
        udp.Client.SendBufferSize = 1 << 20;
        udp.Client.ReceiveBufferSize = 1 << 20;

        var incoming = new ConcurrentQueue<ProtocolPacket>();
        using var signal = new SemaphoreSlim(0);
        using var receiveCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var receiveTask = ReceiveLoopAsync(udp, transferId, incoming, signal, receiveCts.Token);

        var helloAccepted = false;
        try
        {
            progress?.Report(new TransferProgress(0, "Хеширование…"));
            var hash = await ComputeHashAsync(stream, file.Length, progress, cancellationToken);
            stream.Position = 0;

            var hello = new HelloPacket(
                transferId,
                file.Name,
                file.Length,
                ClientSettings.MaxChunkPayload,
                ChunkCount(file.Length, ClientSettings.MaxChunkPayload),
                hash);

            var ack = await HandshakeAsync(udp, remote, hello, incoming, signal, cancellationToken);
            helloAccepted = true;

            var chunkSize = ack.ChunkSize == 0 ? (uint)ClientSettings.MaxChunkPayload : ack.ChunkSize;
            var windowSize = ack.WindowSize == 0 ? (uint)ClientSettings.DefaultWindowSize : ack.WindowSize;
            var totalChunks = ChunkCount(file.Length, chunkSize);

            await TransferBodyAsync(
                udp,
                remote,
                stream,
                transferId,
                chunkSize,
                windowSize,
                totalChunks,
                file.Length,
                hash,
                incoming,
                signal,
                progress,
                cancellationToken);

            progress?.Report(new TransferProgress(100, "Готово"));
        }
        catch (OperationCanceledException)
        {
            if (helloAccepted)
                TrySendAbort(udp, remote, transferId, "передача отменена");
            throw;
        }
        catch (TransferException)
        {
            if (helloAccepted)
                TrySendAbort(udp, remote, transferId, "ошибка клиента");
            throw;
        }
        finally
        {
            receiveCts.Cancel();
            udp.Close();
            try
            {
                await receiveTask;
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
        }
    }

    private static async Task ReceiveLoopAsync(
        UdpClient udp,
        Guid transferId,
        ConcurrentQueue<ProtocolPacket> incoming,
        SemaphoreSlim signal,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            UdpReceiveResult result;
            try
            {
                result = await udp.ReceiveAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }
            catch (SocketException)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;
                continue;
            }

            if (!PacketCodec.TryDecode(result.Buffer, out var packet) || packet is null)
                continue;
            if (packet.TransferId != transferId)
                continue;

            incoming.Enqueue(packet);
            try
            {
                signal.Release();
            }
            catch (ObjectDisposedException)
            {
                break;
            }
        }
    }

    private static async Task<byte[]> ComputeHashAsync(
        FileStream stream,
        long fileSize,
        IProgress<TransferProgress>? progress,
        CancellationToken cancellationToken)
    {
        using var sha = SHA256.Create();
        var buffer = new byte[1024 * 1024];
        long hashed = 0;
        int read;
        while ((read = await stream.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken)) > 0)
        {
            sha.TransformBlock(buffer, 0, read, null, 0);
            hashed += read;
            if (fileSize > 0)
                progress?.Report(new TransferProgress(hashed * 100.0 / fileSize, "Хеширование…"));
        }

        sha.TransformFinalBlock([], 0, 0);
        return sha.Hash ?? new byte[PacketCodec.Sha256Size];
    }

    private static async Task<HelloAckPacket> HandshakeAsync(
        UdpClient udp,
        IPEndPoint remote,
        HelloPacket hello,
        ConcurrentQueue<ProtocolPacket> incoming,
        SemaphoreSlim signal,
        CancellationToken cancellationToken)
    {
        var datagram = PacketCodec.Encode(hello);
        for (var attempt = 0; attempt < ClientSettings.HelloRetries; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            udp.Send(datagram, datagram.Length, remote);

            var ack = await WaitForAsync<HelloAckPacket>(
                incoming,
                signal,
                ClientSettings.HelloAckTimeout,
                cancellationToken);
            if (ack is null)
                continue;

            if (ack.Status != HelloAckStatus.Ok)
            {
                var reason = string.IsNullOrWhiteSpace(ack.Reason) ? "сервер отклонил передачу" : ack.Reason;
                throw new TransferException(reason);
            }

            return ack;
        }

        throw new TransferException("Сервер не ответил на Hello.");
    }

    private static async Task TransferBodyAsync(
        UdpClient udp,
        IPEndPoint remote,
        FileStream stream,
        Guid transferId,
        uint chunkSize,
        uint windowSize,
        uint totalChunks,
        long fileSize,
        byte[] hash,
        ConcurrentQueue<ProtocolPacket> incoming,
        SemaphoreSlim signal,
        IProgress<TransferProgress>? progress,
        CancellationToken cancellationToken)
    {
        uint firstMissing = 0;
        uint nextToSend = 0;
        var inFlight = new HashSet<uint>();
        var lastBitmap = Array.Empty<byte>();
        var chunkBuffer = new byte[Math.Max(chunkSize, 1)];
        var fin = PacketCodec.Encode(new FinPacket(transferId, totalChunks, hash));
        var finAttempts = 0;

        void Report()
        {
            var percent = totalChunks == 0 ? 100 : firstMissing * 100.0 / totalChunks;
            progress?.Report(new TransferProgress(percent, "Отправка…"));
        }

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            DrainIncoming(incoming, ref firstMissing, ref lastBitmap, inFlight, out var finAck);
            ConsumeSignals(signal);
            Report();

            if (finAck is not null)
            {
                if (HandleFinAck(finAck, ref firstMissing, ref lastBitmap, inFlight))
                    return;
                finAttempts = 0;
                continue;
            }

            if (firstMissing < totalChunks)
            {
                RetransmitKnownHoles(
                    udp, remote, stream, transferId, chunkSize, fileSize,
                    firstMissing, nextToSend, lastBitmap, inFlight, chunkBuffer, windowSize);
                FillWindow(
                    udp, remote, stream, transferId, chunkSize, fileSize,
                    windowSize, totalChunks, firstMissing, ref nextToSend, inFlight, chunkBuffer);

                var gotSignal = await signal.WaitAsync(ClientSettings.SackTimeout, cancellationToken);
                if (!gotSignal)
                {
                    foreach (var seq in inFlight.ToArray())
                        SendChunk(udp, remote, stream, transferId, chunkSize, fileSize, seq, chunkBuffer);
                }

                continue;
            }

            if (finAttempts >= ClientSettings.FinRetries)
                throw new TransferException("Сервер не подтвердил завершение передачи.");

            finAttempts++;
            udp.Send(fin, fin.Length, remote);
            var ack = await WaitForAsync<FinAckPacket>(
                incoming,
                signal,
                ClientSettings.FinAckTimeout,
                cancellationToken);
            if (ack is null)
                continue;

            if (HandleFinAck(ack, ref firstMissing, ref lastBitmap, inFlight))
                return;
            finAttempts = 0;
        }
    }

    private static bool HandleFinAck(
        FinAckPacket ack,
        ref uint firstMissing,
        ref byte[] lastBitmap,
        HashSet<uint> inFlight)
    {
        switch (ack.Status)
        {
            case FinAckStatus.Ok:
                return true;
            case FinAckStatus.Incomplete:
                ApplySack(ack.FirstMissing, ack.Bitmap, ref firstMissing, ref lastBitmap, inFlight);
                return false;
            case FinAckStatus.HashMismatch:
                throw new TransferException("Не совпал SHA-256.");
            case FinAckStatus.Error:
                throw new TransferException("Сервер не смог сохранить файл.");
            default:
                throw new TransferException("Неизвестный ответ FinAck.");
        }
    }

    private static void FillWindow(
        UdpClient udp,
        IPEndPoint remote,
        FileStream stream,
        Guid transferId,
        uint chunkSize,
        long fileSize,
        uint windowSize,
        uint totalChunks,
        uint firstMissing,
        ref uint nextToSend,
        HashSet<uint> inFlight,
        byte[] chunkBuffer)
    {
        while (inFlight.Count < windowSize && nextToSend < totalChunks && nextToSend < firstMissing + windowSize)
        {
            SendChunk(udp, remote, stream, transferId, chunkSize, fileSize, nextToSend, chunkBuffer);
            inFlight.Add(nextToSend);
            nextToSend++;
        }
    }

    private static void RetransmitKnownHoles(
        UdpClient udp,
        IPEndPoint remote,
        FileStream stream,
        Guid transferId,
        uint chunkSize,
        long fileSize,
        uint firstMissing,
        uint nextToSend,
        byte[] bitmap,
        HashSet<uint> inFlight,
        byte[] chunkBuffer,
        uint windowSize)
    {
        if (nextToSend == 0 || firstMissing >= nextToSend)
            return;

        var limit = Math.Min(nextToSend, firstMissing + windowSize);
        for (var seq = firstMissing; seq < limit; seq++)
        {
            if (!IsKnownHole(seq, firstMissing, bitmap))
                continue;

            SendChunk(udp, remote, stream, transferId, chunkSize, fileSize, seq, chunkBuffer);
            inFlight.Add(seq);
        }
    }

    private static void SendChunk(
        UdpClient udp,
        IPEndPoint remote,
        FileStream stream,
        Guid transferId,
        uint chunkSize,
        long fileSize,
        uint seq,
        byte[] chunkBuffer)
    {
        var size = ExpectedPayloadSize(seq, chunkSize, fileSize);
        stream.Position = (long)seq * chunkSize;
        stream.ReadExactly(chunkBuffer, 0, (int)size);
        var payload = new byte[size];
        Buffer.BlockCopy(chunkBuffer, 0, payload, 0, (int)size);
        var datagram = PacketCodec.Encode(new DataPacket(transferId, seq, payload));
        udp.Send(datagram, datagram.Length, remote);
    }

    private static void DrainIncoming(
        ConcurrentQueue<ProtocolPacket> incoming,
        ref uint firstMissing,
        ref byte[] lastBitmap,
        HashSet<uint> inFlight,
        out FinAckPacket? finAck)
    {
        finAck = null;
        while (incoming.TryDequeue(out var packet))
        {
            switch (packet)
            {
                case AbortPacket abort:
                    throw new TransferException(string.IsNullOrWhiteSpace(abort.Reason) ? "Сервер прервал передачу." : abort.Reason);
                case SackPacket sack:
                    ApplySack(sack.FirstMissing, sack.Bitmap, ref firstMissing, ref lastBitmap, inFlight);
                    break;
                case FinAckPacket ack:
                    finAck = ack;
                    break;
            }
        }
    }

    private static void ApplySack(
        uint sackFirstMissing,
        byte[] bitmap,
        ref uint firstMissing,
        ref byte[] lastBitmap,
        HashSet<uint> inFlight)
    {
        if (sackFirstMissing < firstMissing)
            return;

        firstMissing = sackFirstMissing;
        lastBitmap = bitmap;
        var ackedUpTo = firstMissing;
        var ackedBits = bitmap;
        inFlight.RemoveWhere(seq => IsAcked(seq, ackedUpTo, ackedBits));
    }

    private static bool IsAcked(uint seq, uint firstMissing, byte[] bitmap)
    {
        if (seq < firstMissing)
            return true;
        if (seq == firstMissing)
            return false;

        var bitIndex = (int)(seq - firstMissing - 1);
        if (bitIndex < 0 || bitIndex / 8 >= bitmap.Length)
            return false;

        return (bitmap[bitIndex / 8] & (1 << (bitIndex % 8))) != 0;
    }

    private static bool IsKnownHole(uint seq, uint firstMissing, byte[] bitmap)
    {
        if (seq < firstMissing)
            return false;
        if (seq == firstMissing)
            return true;

        var bitIndex = (int)(seq - firstMissing - 1);
        if (bitIndex < 0 || bitIndex / 8 >= bitmap.Length)
            return false;

        return (bitmap[bitIndex / 8] & (1 << (bitIndex % 8))) == 0;
    }

    private static async Task<T?> WaitForAsync<T>(
        ConcurrentQueue<ProtocolPacket> incoming,
        SemaphoreSlim signal,
        TimeSpan timeout,
        CancellationToken cancellationToken)
        where T : ProtocolPacket
    {
        var skipped = new List<ProtocolPacket>();
        var deadline = DateTime.UtcNow + timeout;
        try
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                while (incoming.TryDequeue(out var packet))
                {
                    if (packet is AbortPacket abort)
                        throw new TransferException(string.IsNullOrWhiteSpace(abort.Reason) ? "Сервер прервал передачу." : abort.Reason);
                    if (packet is T typed)
                        return typed;
                    skipped.Add(packet);
                }

                var remaining = deadline - DateTime.UtcNow;
                if (remaining <= TimeSpan.Zero)
                    return null;

                if (!await signal.WaitAsync(remaining, cancellationToken))
                    return null;
            }
        }
        finally
        {
            foreach (var packet in skipped)
                incoming.Enqueue(packet);
        }
    }

    private static void ConsumeSignals(SemaphoreSlim signal)
    {
        while (signal.Wait(TimeSpan.Zero))
        {
        }
    }

    private static void TrySendAbort(UdpClient udp, IPEndPoint remote, Guid transferId, string reason)
    {
        try
        {
            var datagram = PacketCodec.Encode(new AbortPacket(transferId, reason));
            udp.Send(datagram, datagram.Length, remote);
        }
        catch
        {
            // Сокет уже мог быть закрыт.
        }
    }

    private static uint ChunkCount(long fileSize, uint chunkSize)
    {
        if (fileSize <= 0 || chunkSize == 0)
            return 0;
        return (uint)((fileSize + chunkSize - 1) / chunkSize);
    }

    private static uint ExpectedPayloadSize(uint seq, uint chunkSize, long fileSize)
    {
        var offset = (long)seq * chunkSize;
        var remaining = fileSize - offset;
        if (remaining <= 0)
            return 0;
        return remaining < chunkSize ? (uint)remaining : chunkSize;
    }
}
