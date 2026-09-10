namespace LocalFlowWpfClient;

internal static class ClientSettings
{
    public const int DefaultPort = 45123;
    public const int HeaderSize = 26;
    public const int StreamBufferSize = 64 * 1024;
    public const int MaxControlPayload = 1024;
    public const int SocketBufferSize = 1 << 20;

    public const long MaxFileSizeBytes = 10L * 1024 * 1024 * 1024;
    public const int MaxFileNameBytes = 255;
    public const string ServiceType = "_localflow._tcp";

    public static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan HelloAckTimeout = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan FinAckTimeout = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan IdleTimeout = TimeSpan.FromSeconds(60);
}
