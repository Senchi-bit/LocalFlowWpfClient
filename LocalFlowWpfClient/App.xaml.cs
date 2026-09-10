using System.Net;
using System.Windows;
using LocalFlowWpfClient.Net;

namespace LocalFlowWpfClient;

public partial class App : Application
{
    protected override async void OnStartup(StartupEventArgs e)
    {
        base.OnStartup(e);

        if (e.Args is ["--send", var file, var dest])
        {
            ShutdownMode = ShutdownMode.OnExplicitShutdown;
            try
            {
                if (!TryParseEndpoint(dest, out var endpoint))
                    throw new InvalidOperationException($"Некорректный адрес: {dest}");

                await TcpFileSender.SendAsync(file, endpoint, null, CancellationToken.None);
                Shutdown(0);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
                Shutdown(1);
            }

            return;
        }

        new MainWindow().Show();
    }

    private static bool TryParseEndpoint(string text, out IPEndPoint endpoint)
    {
        if (IPEndPoint.TryParse(text, out endpoint!))
        {
            if (endpoint.Port == 0)
                endpoint = new IPEndPoint(endpoint.Address, ClientSettings.DefaultPort);
            return true;
        }

        if (IPAddress.TryParse(text, out var address))
        {
            endpoint = new IPEndPoint(address, ClientSettings.DefaultPort);
            return true;
        }

        endpoint = new IPEndPoint(IPAddress.None, 0);
        return false;
    }
}
