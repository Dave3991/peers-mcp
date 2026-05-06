using System.Net;
using FluentResults;

namespace Peers.Broker.Errors;

public sealed class NotFoundError(string message) : Error(message)
{
    public HttpStatusCode StatusCode => HttpStatusCode.NotFound;
}

public sealed class BadRequestError(string message) : Error(message)
{
    public HttpStatusCode StatusCode => HttpStatusCode.BadRequest;
}

public sealed class ConflictError(string message) : Error(message)
{
    public HttpStatusCode StatusCode => HttpStatusCode.Conflict;
}

public sealed class PayloadTooLargeError(string message) : Error(message)
{
    public HttpStatusCode StatusCode => HttpStatusCode.RequestEntityTooLarge;
}

public static class ResultHttp
{
    public static IResult ToHttp(this Result result, IResult onSuccess) =>
        result.IsSuccess ? onSuccess : ToError(result.Errors);

    public static IResult ToHttp(this Result result) =>
        result.IsSuccess ? Results.Ok(new { ok = true }) : ToError(result.Errors);

    public static IResult ToHttp<T>(this Result<T> result, Func<T, IResult>? onSuccess = null)
    {
        if (result.IsFailed) return ToError(result.Errors);
        return onSuccess is null ? Results.Ok(result.Value) : onSuccess(result.Value);
    }

    private static IResult ToError(IReadOnlyCollection<IError> errors)
    {
        var first = errors.FirstOrDefault();
        var (status, message) = first switch
        {
            NotFoundError e => ((int)e.StatusCode, e.Message),
            BadRequestError e => ((int)e.StatusCode, e.Message),
            ConflictError e => ((int)e.StatusCode, e.Message),
            PayloadTooLargeError e => ((int)e.StatusCode, e.Message),
            { } e => ((int)HttpStatusCode.BadRequest, e.Message),
            _ => ((int)HttpStatusCode.BadRequest, "Unknown error."),
        };
        return Results.Json(new { error = message }, statusCode: status);
    }
}
