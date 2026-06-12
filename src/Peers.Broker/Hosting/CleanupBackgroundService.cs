using Peers.Broker.Services;

namespace Peers.Broker.Hosting;

public sealed class CleanupBackgroundService(
    IServiceProvider services,
    ILogger<CleanupBackgroundService> logger,
    BrokerStatus brokerStatus) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var cycle = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            cycle++;
            try
            {
                using var scope = services.CreateScope();
                var peerService = scope.ServiceProvider.GetRequiredService<PeerService>();
                var eventService = scope.ServiceProvider.GetRequiredService<EventService>();

                var expired = await peerService.ExpireStaleAsync(stoppingToken);
                if (expired > 0)
                    logger.LogInformation("Expired {Count} stale peers", expired);

                if (cycle % 10 == 0)
                {
                    var purged = await peerService.PurgeExpiredAsync(stoppingToken);
                    if (purged > 0)
                        logger.LogInformation("Purged {Count} expired peers", purged);

                    var capped = await eventService.PruneInboxAsync(stoppingToken);
                    if (capped > 0)
                        logger.LogWarning("Capped {Count} unacked inbox rows for over-quota peer(s)", capped);
                }

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
