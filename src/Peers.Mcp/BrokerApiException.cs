using System.Net;

namespace Peers.Mcp;

public sealed class BrokerApiException : Exception
{
    public HttpStatusCode StatusCode { get; }
    public string? ErrorBody { get; }

    public BrokerApiException(HttpStatusCode statusCode, string? errorBody)
        : base($"Broker returned {(int)statusCode} {statusCode}" + (errorBody is not null ? $": {errorBody}" : ""))
    {
        StatusCode = statusCode;
        ErrorBody = errorBody;
    }

    private BrokerApiException(string message, Exception inner)
        : base(message, inner)
    {
        StatusCode = HttpStatusCode.ServiceUnavailable;
        ErrorBody = inner.Message;
    }

    public static BrokerApiException FromTransport(HttpRequestException ex) =>
        new($"Broker is unreachable: {ex.Message}. Is the broker running?", ex);
}
