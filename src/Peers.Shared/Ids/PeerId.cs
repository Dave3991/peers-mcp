namespace Peers.Shared.Ids;

public readonly record struct PeerId(string Value)
{
    public static PeerId New() => new(Guid.NewGuid().ToString("N")[..16]);
    public override string ToString() => Value;
}
