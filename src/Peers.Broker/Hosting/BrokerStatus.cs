namespace Peers.Broker.Hosting;

/// <summary>
/// Tracks lightweight broker process metadata for the /status endpoint.
/// Both reads and writes are lock-free via Interlocked.
/// </summary>
public sealed class BrokerStatus
{
    private long _lastCleanupAtTicks;

    public BrokerStatus()
    {
        ProcessStartTime = DateTimeOffset.UtcNow;
    }

    public DateTimeOffset ProcessStartTime { get; }

    public DateTimeOffset? LastCleanupAt
    {
        get
        {
            var ticks = Interlocked.Read(ref _lastCleanupAtTicks);
            return ticks == 0 ? null : new DateTimeOffset(ticks, TimeSpan.Zero);
        }
    }

    public void MarkCleanupCompleted(DateTimeOffset at)
    {
        Interlocked.Exchange(ref _lastCleanupAtTicks, at.UtcTicks);
    }
}
