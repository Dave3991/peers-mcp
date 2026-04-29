using Peers.Broker.Services;

namespace Peers.Broker.Hosting;

public sealed class CleanupBackgroundService(
    IServiceProvider services,
    ILogger<CleanupBackgroundService> logger,
    BrokerStatus brokerStatus) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = services.CreateScope();
                var peerService = scope.ServiceProvider.GetRequiredService<PeerService>();
                var eventService = scope.ServiceProvider.GetRequiredService<EventService>();

                var expired = await peerService.ExpireStaleAsync(stoppingToken);
                if (expired > 0)
                    logger.LogInformation("Expired {Count} stale peers", expired);

                await eventService.PruneAsync(stoppingToken);

                brokerStatus.MarkCleanupCompleted(DateTimeOffset.UtcNow);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Cleanup cycle failed");
            }

            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
        }
    }
}
