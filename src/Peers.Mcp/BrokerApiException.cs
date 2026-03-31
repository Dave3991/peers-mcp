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
}
