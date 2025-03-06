namespace Yarkool.Hangfire.Redis.ServiceStack;

public interface IQueueCommand
{
    object?[]? GetPipelineResults();

    object?[]? GetTransactionResults();
}