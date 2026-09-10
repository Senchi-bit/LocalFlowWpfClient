namespace LocalFlowWpfClient;

internal static class ClientSettings
{
    public const int DefaultPort = 45123;
    public const int HeaderSize = 22;
    public const int MaxChunkPayload = 1200;
    public const int DefaultWindowSize = 64;
    public const long MaxFileSizeBytes = 10L * 1024 * 1024 * 1024;
    public const int MaxFileNameBytes = 255;
    public const string ServiceType = "_localflow._udp";

    public const int HelloRetries = 3;
    public const int FinRetries = 5;

    public static readonly TimeSpan HelloAckTimeout = TimeSpan.FromSeconds(1);
    public static readonly TimeSpan SackTimeout = TimeSpan.FromMilliseconds(400);
    public static readonly TimeSpan FinAckTimeout = TimeSpan.FromSeconds(1);
}
