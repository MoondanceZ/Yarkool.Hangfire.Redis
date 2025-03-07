using ServiceStack.Redis;
using ServiceStack.Redis.Pipeline;

namespace Yarkool.Hangfire.Redis.ServiceStack;

public static class QueueCommandExtensions
{
    public static void QueueCommand<T>(this IRedisQueueableOperation operation, Func<global::ServiceStack.Redis.IRedisClient, T> func, List<object> resultList)
    {
        if (typeof(T) == typeof(int))
        {
            var intFunc = func as Func<global::ServiceStack.Redis.IRedisClient, int>;
            operation.QueueCommand(intFunc, s => resultList.Add(s));
        }
        else if (typeof(T) == typeof(long))
        {
            var longFunc = func as Func<global::ServiceStack.Redis.IRedisClient, long>;
            operation.QueueCommand(longFunc, s => resultList.Add(s));
        }
        else if (typeof(T) == typeof(bool))
        {
            var boolFunc = func as Func<global::ServiceStack.Redis.IRedisClient, bool>;
            operation.QueueCommand(boolFunc, s => resultList.Add(s));
        }
        else if (typeof(T) == typeof(double))
        {
            var doubleFunc = func as Func<global::ServiceStack.Redis.IRedisClient, double>;
            operation.QueueCommand(doubleFunc, s => resultList.Add(s));
        }
        else if (typeof(T) == typeof(string))
        {
            var stringFunc = func as Func<global::ServiceStack.Redis.IRedisClient, string>;
            operation.QueueCommand(stringFunc, s => resultList.Add(s));
        }
        else
        {
            throw new NotSupportedException($"Not supported type: {typeof(T)}");
        }
    }

    public static void QueueCommandAsync<T>(this IRedisQueueableOperationAsync operation, Func<IRedisClientAsync, Task<T>> func, List<object> resultList)
    {
        Func<IRedisClientAsync, ValueTask<T>> convertedFunc = async client =>
        {
            Task<T> task = func(client);
            if (task.IsCompleted)
            {
                return task.Result;
            }

            return await task;
        };

        if (typeof(T) == typeof(int))
        {
            var intFunc = convertedFunc as Func<IRedisClientAsync, ValueTask<int>>;
            operation.QueueCommand(intFunc, s => resultList.Add(s));
        }
        else if (typeof(T) == typeof(long))
        {
            var longFunc = convertedFunc as Func<IRedisClientAsync, ValueTask<long>>;
            operation.QueueCommand(longFunc, s => resultList.Add(s));
        }
        else if (typeof(T) == typeof(bool))
        {
            var boolFunc = convertedFunc as Func<IRedisClientAsync, ValueTask<bool>>;
            operation.QueueCommand(boolFunc, s => resultList.Add(s));
        }
        else if (typeof(T) == typeof(double))
        {
            var doubleFunc = convertedFunc as Func<IRedisClientAsync, ValueTask<double>>;
            operation.QueueCommand(doubleFunc, s => resultList.Add(s));
        }
        else if (typeof(T) == typeof(string))
        {
            var stringFunc = convertedFunc as Func<IRedisClientAsync, ValueTask<string>>;
            operation.QueueCommand(stringFunc, s => resultList.Add(s));
        }
        else
        {
            throw new NotSupportedException($"Not supported type: {typeof(T)}");
        }
    }
}