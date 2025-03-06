using ServiceStack.Redis;

namespace Yarkool.Hangfire.Redis.ServiceStack;

public partial class ServiceStackCommand : IRedisCommand
{
    private readonly IRedisTransaction? _redisTransaction;
    private readonly IRedisPipeline? _redisPipeline;
    private readonly IRedisClientsManager? _redisClientsManager;

    public ServiceStackCommand(IRedisClientsManager? redisClientsManager)
    {
        _redisClientsManager = redisClientsManager;
    }
    
    public ServiceStackCommand(IRedisPipeline? redisPipeline)
    {
        _redisPipeline = redisPipeline;
    }
    
    public ServiceStackCommand(IRedisTransaction? redisTransaction)
    {
        _redisTransaction = redisTransaction;
    }
    
    private void UseClient(Action<global::ServiceStack.Redis.IRedisClient> action)
    {
        if (_redisClientsManager != null)
        {
            using var redisClient = _redisClientsManager.GetClient();
            action.Invoke(redisClient);
        }
        else if (_redisPipeline != null)
        {
            _redisPipeline.QueueCommand(action);
        }
    }

    private T UseClient<T>(Func<global::ServiceStack.Redis.IRedisClient, T> func)
    {
        if (_redisClientsManager != null)
        {
            using var redisClient = _redisClientsManager.GetClient();
            return func.Invoke(redisClient);
        }
    }

    private async Task<T> UseClientAsync<T>(Func<global::ServiceStack.Redis.IRedisClientAsync, Task<T>> func)
    {
        if (_redisClientsManager != null)
        {
            await using var redisClient = await _redisClientsManager.GetClientAsync().ConfigureAwait(false);
            return await func.Invoke(redisClient);
        }
    }

    private async Task UseClientAsync(Action<global::ServiceStack.Redis.IRedisClientAsync> action)
    {
        if (_redisClientsManager != null)
        {
            await using var redisClient = await _redisClientsManager.GetClientAsync().ConfigureAwait(false);
            action.Invoke(redisClient);
        }
    }

    public long Publish(string channel, string message) => UseClient(x => x.PublishMessage(channel, message));

    public Task<long> PublishAsync(string channel, string message) => UseClientAsync(async x => await x.PublishMessageAsync(channel, message).ConfigureAwait(false));

    public IDisposable? Subscribe(string key, Action<string, object> handler) => new RedisPubSubServer(redisClientsManager, key) { OnMessage = handler }.Start();

    public void UnSubscribe(string key) => new RedisPubSubServer(redisClientsManager, key).Stop();

    public string? Get(string key) => UseClient(x => x.GetValue(key));

    public Task<string?> GetAsync(string key) => UseClientAsync(async x => await x.GetValueAsync(key).ConfigureAwait(false));

    public string?[] MGet(params string[] keys) => UseClient(x => x.GetValues(keys.ToList()).ToArray());

    public bool Set(string key, string value)
    {
        return UseClient(x =>
        {
            x.SetValue(key, value);
            return true;
        });
    }

    public Task<bool> SetAsync(string key, string value)
    {
        return UseClientAsync(async x =>
        {
            await x.SetValueAsync(key, value).ConfigureAwait(false);
            return true;
        });
    }

    public bool Set(string key, string value, TimeSpan expiry)
    {
        return UseClient(x =>
        {
            x.SetValue(key, value, expiry);
            return true;
        });
    }

    public Task<bool> SetAsync(string key, string value, TimeSpan expiry)
    {
        return UseClientAsync(async x =>
        {
            await x.SetValueAsync(key, value, expiry).ConfigureAwait(false);
            return true;
        });
    }

    public bool SetNx(string key, string value) => UseClient(x => x.SetValueIfNotExists(key, value));

    public Task<bool> SetNxAsync(string key, string value) => UseClientAsync(async x => await x.SetValueIfNotExistsAsync(key, value).ConfigureAwait(false));

    public bool SetNx(string key, string value, TimeSpan expiry) => UseClient(x => x.SetValueIfNotExists(key, value, expiry));

    public Task<bool> SetNxAsync(string key, string value, TimeSpan expiry) => UseClientAsync(async x => await x.SetValueIfNotExistsAsync(key, value, expiry).ConfigureAwait(false));

    public long ZAdd(string key, string member, decimal score) =>
        UseClient(x =>
        {
            x.AddItemToSortedSet(key, member, (double)score);
            return 1L;
        });

    public Task<long> ZAddAsync(string key, string member, decimal score) =>
        UseClientAsync(async x =>
        {
            await x.AddItemToSortedSetAsync(key, member, (double)score).ConfigureAwait(false);
            return 1L;
        });

    public long ZAdd(string key, Dictionary<string, decimal> data) =>
        UseClient(x =>
        {
            var pipeline = x.CreatePipeline();
            foreach (var item in data)
            {
                pipeline.QueueCommand(s => s.AddItemToSortedSet(key, item.Key, (double)item.Value));
            }

            pipeline.Flush();
            return data.Count;
        });

    public Task<long> ZAddAsync(string key, Dictionary<string, decimal> data) =>
        UseClientAsync(async x =>
        {
            var pipeline = x.CreatePipeline();
            foreach (var item in data)
            {
                pipeline.QueueCommand(async s => await s.AddItemToSortedSetAsync(key, item.Key, (double)item.Value).ConfigureAwait(false));
            }

            await pipeline.FlushAsync();
            return (long)data.Count;
        });

    public long ZRem(string key, params string[] members) => UseClient(x => x.RemoveItemsFromSortedSet(key, members.ToList()));

    public Task<long> ZRemAsync(string key, params string[] members) => UseClientAsync(async x => await x.RemoveItemsFromSortedSetAsync(key, members.ToList()).ConfigureAwait(false));

    public long ZCard(string key) => UseClient(x => x.GetSortedSetCount(key));

    public Task<long> ZCardAsync(string key) => UseClientAsync(async x => await x.GetSortedSetCountAsync(key).ConfigureAwait(false));

    public long ZCount(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue) => UseClient(x => x.GetSortedSetCount(key, (double)min, (double)max));

    public Task<long> ZCountAsync(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue) => UseClientAsync(async x => await x.GetSortedSetCountAsync(key, (double)min, (double)max).ConfigureAwait(false));

    public IRedisCommand.ScanResult<IRedisCommand.ZMember> ZScan(string key, long cursor, string? pattern = null, long count = 0)
    {
        return UseClient(x =>
        {
            var result = ((IRedisNativeClient)x).ZScan(key, (ulong)cursor, (int)count, pattern);
            return new IRedisCommand.ScanResult<IRedisCommand.ZMember>((long)result.Cursor, result.AsItemsWithScores()?.Select(s => new IRedisCommand.ZMember(s.Key, (decimal)s.Value)).ToArray() ?? []);
        });
    }

    public string[] ZRangeByScore(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue, int skip = 0, int take = 0) => UseClient(x => x.GetRangeFromSortedSetByLowestScore(key, (double)min, (double)max, skip, take)?.ToArray() ?? []);

    public Task<string[]> ZRangeByScoreAsync(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue, int skip = 0, int take = 0) => UseClientAsync(async x => (await x.GetRangeFromSortedSetByLowestScoreAsync(key, (double)min, (double)max, skip, take).ConfigureAwait(false))?.ToArray() ?? []);

    public string[] ZRangeByRank(string key, long start, long stop) => UseClient(x => x.GetRangeFromSortedSet(key, (int)start, (int)stop)?.ToArray() ?? []);

    public Task<string[]> ZRangeByRankAsync(string key, long start, long stop) => UseClientAsync(async x => (await x.GetRangeFromSortedSetAsync(key, (int)start, (int)stop).ConfigureAwait(false))?.ToArray() ?? []);

    public string[] ZRevRangeByRank(string key, long start, long stop) => UseClient(x => x.GetRangeFromSortedSetDesc(key, (int)start, (int)stop)?.ToArray() ?? []);

    public Task<string[]> ZRevRangeByRankAsync(string key, long start, long stop) => UseClientAsync(async x => (await x.GetRangeFromSortedSetDescAsync(key, (int)start, (int)stop).ConfigureAwait(false))?.ToArray() ?? []);

    public (string Element, decimal Score)[] ZRangeByRankWithScores(string key, long start, long stop) => UseClient(x => x.GetRangeWithScoresFromSortedSet(key, (int)start, (int)stop)?.Select(s => (s.Key, (decimal)s.Value)).ToArray() ?? []);

    public Task<(string Element, decimal Score)[]> ZRangeByRankWithScoresAsync(string key, long start, long stop) => UseClientAsync(async x => (await x.GetRangeWithScoresFromSortedSetAsync(key, (int)start, (int)stop).ConfigureAwait(false))?.Select(s => (s.Key, (decimal)s.Value)).ToArray() ?? []);

    public (string Element, decimal Score)[] ZRangeByScoreWithScores(string key, decimal min, decimal max, long offset = 0, long count = 0) => UseClient(x => x.GetRangeWithScoresFromSortedSetByLowestScore(key, (double)min, (double)max, (int)offset, (int)count)?.Select(s => (s.Key, (decimal)s.Value)).ToArray() ?? []);

    public Task<(string Element, decimal Score)[]> ZRangeByScoreWithScoresAsync(string key, decimal min, decimal max, long offset = 0, long count = 0) => UseClientAsync(async x => (await x.GetRangeWithScoresFromSortedSetByLowestScoreAsync(key, (double)min, (double)max, (int)offset, (int)count).ConfigureAwait(false))?.Select(s => (s.Key, (decimal)s.Value)).ToArray() ?? []);

    public long? ZRank(string key, string member) => UseClient(x => x.GetItemIndexInSortedSet(key, member));

    public Task<long?> ZRankAsync(string key, string member) => UseClientAsync(async x => (long?)await x.GetItemIndexInSortedSetAsync(key, member).ConfigureAwait(false));

    public decimal? ZScore(string key, string member) => UseClient(x => (decimal?)x.GetItemScoreInSortedSet(key, member));

    public Task<decimal?> ZScoreAsync(string key, string member) => UseClientAsync(async x => (decimal?)await x.GetItemScoreInSortedSetAsync(key, member).ConfigureAwait(false));

    public long SAdd(string key, params string[] members) =>
        UseClient(x =>
        {
            x.AddRangeToSet(key, members.ToList());
            return members.Length;
        });

    public Task<long> SAddAsync(string key, params string[] members) =>
        UseClientAsync(async x =>
        {
            await x.AddRangeToSetAsync(key, members.ToList()).ConfigureAwait(false);
            return (long)members.Length;
        });

    public long SRem(string key, string member) =>
        UseClient(x =>
        {
            x.RemoveItemFromSet(key, member);
            return 1L;
        });

    public Task<long> SRemAsync(string key, string member) =>
        UseClientAsync(async x =>
        {
            await x.RemoveItemFromSetAsync(key, member).ConfigureAwait(false);
            return 1L;
        });

    public string[] SMembers(string key) => UseClient(x => x.GetAllItemsFromSet(key)?.ToArray() ?? []);

    public Task<string[]> SMembersAsync(string key) => UseClientAsync(async x => (await x.GetAllItemsFromSetAsync(key).ConfigureAwait(false))?.ToArray() ?? []);

    public bool SIsMember(string key, string member) => UseClient(x => x.SetContainsItem(key, member));

    public Task<bool> SIsMemberAsync(string key, string member) => UseClientAsync(async x => await x.SetContainsItemAsync(key, member).ConfigureAwait(false));

    public long SCard(string key) => UseClient(x => x.GetSetCount(key));

    public Task<long> SCardAsync(string key) => UseClientAsync(async x => await x.GetSetCountAsync(key).ConfigureAwait(false));

    public bool Expire(string key, TimeSpan expiry) => UseClient(x => x.ExpireEntryIn(key, expiry));

    public Task<bool> ExpireAsync(string key, TimeSpan expiry) => UseClientAsync(async x => await x.ExpireEntryInAsync(key, expiry).ConfigureAwait(false));

    public bool Persist(string key) => UseClient(x => ((IRedisNativeClient)x).Persist(key));

    public Task<bool> PersistAsync(string key) => UseClientAsync(async x => await ((IRedisNativeClientAsync)x).PersistAsync(key).ConfigureAwait(false));

    public long Del(params string[] keys) =>
        UseClient(x =>
        {
            x.RemoveAll(keys);
            return keys.Length;
        });

    public Task<long> DelAsync(params string[] keys) =>
        UseClientAsync(async x =>
        {
            await x.RemoveAllAsync(keys).ConfigureAwait(false);
            return (long)keys.Length;
        });

    public long HSet(string key, string field, string value) =>
        UseClient(x =>
        {
            x.SetEntryInHash(key, field, value);
            return 1L;
        });

    public Task<long> HSetAsync(string key, string field, string value) =>
        UseClientAsync(async x =>
        {
            await x.SetEntryInHashAsync(key, field, value).ConfigureAwait(false);
            return 1L;
        });

    public long HSet(string key, Dictionary<string, string> keyValues) =>
        UseClient(x =>
        {
            x.SetRangeInHash(key, keyValues);
            return keyValues.LongCount();
        });

    public Task<long> HSetAsync(string key, Dictionary<string, string> keyValues) =>
        UseClientAsync(async x =>
        {
            await x.SetRangeInHashAsync(key, keyValues).ConfigureAwait(false);
            return keyValues.LongCount();
        });

    public string? HGet(string key, string field) => UseClient(x => x.GetValueFromHash(key, field));

    public Task<string?> HGetAsync(string key, string field) => UseClientAsync(async x => await x.GetValueFromHashAsync(key, field).ConfigureAwait(false));

    public string?[]? HMGet(string key, params string[] fields)
    {
        return UseClient(x =>
        {
            var result = x.GetValuesFromHash(key, fields) ?? [];
            var resultCount = result.Count;
            if (resultCount < fields.Length)
            {
                for (var i = 0; i < fields.Length - resultCount; i++)
                {
                    result.Add(null);
                }
            }

            return result.ToArray();
        });
    }

    public Task<string?[]?> HMGetAsync(string key, params string[] fields)
    {
        #pragma warning disable CS8619
        return UseClientAsync(async x =>
        {
            var result = await x.GetValuesFromHashAsync(key, fields).ConfigureAwait(false) ?? [];
            var resultCount = result.Count;
            if (resultCount < fields.Length)
            {
                for (var i = 0; i < fields.Length - resultCount; i++)
                {
                    result.Add(null);
                }
            }

            return result.Select(s => s).ToArray();
        });
        #pragma warning restore CS8619
    }

    public Dictionary<string, string> HGetAll(string key) => UseClient(x => x.GetAllEntriesFromHash(key));

    public long HLen(string key) => UseClient(x => x.GetHashCount(key));

    public Task<long> HLenAsync(string key) => UseClientAsync(async x => await x.GetHashCountAsync(key).ConfigureAwait(false));

    public long HDel(string key, params string[] fields) =>
        UseClient(x =>
        {
            var pipeline = x.CreatePipeline();
            foreach (var field in fields)
            {
                pipeline.QueueCommand(s => s.RemoveEntryFromHash(key, field));
            }

            pipeline.Flush();
            return fields.LongLength;
        });

    public Task<long> HDelAsync(string key, params string[] fields) =>
        UseClientAsync(async x =>
        {
            var pipeline = x.CreatePipeline();
            foreach (var field in fields)
            {
                pipeline.QueueCommand(async s => await s.RemoveEntryFromHashAsync(key, field).ConfigureAwait(false));
            }

            await pipeline.FlushAsync();
            return fields.LongLength;
        });

    public bool HExists(string key, string field) => UseClient(x => x.HashContainsEntry(key, field));

    public Task<bool> HExistsAsync(string key, string field) => UseClientAsync(async x => await x.HashContainsEntryAsync(key, field).ConfigureAwait(false));

    public long RPush(string key, params string[] elements) =>
        UseClient(x =>
        {
            x.AddRangeToList(key, elements.ToList());
            return elements.LongLength;
        });

    public Task<long> RPushAsync(string key, params string[] elements) =>
        UseClientAsync(async x =>
        {
            await x.AddRangeToListAsync(key, elements.ToList()).ConfigureAwait(false);
            return elements.LongLength;
        });

    public long LPush(string key, params string[] elements) =>
        UseClient(x =>
        {
            x.PrependRangeToList(key, elements.ToList());
            return elements.LongLength;
        });

    public Task<long> LPushAsync(string key, params string[] elements) =>
        UseClientAsync(async x =>
        {
            await x.PrependRangeToListAsync(key, elements.ToList()).ConfigureAwait(false);
            return elements.LongLength;
        });

    public string? RPopLPush(string source, string destination) => UseClient(x => x.PopAndPushItemBetweenLists(source, destination));

    public Task<string?> RPopLPushAsync(string source, string destination) => UseClientAsync(async x => await x.PopAndPushItemBetweenListsAsync(source, destination).ConfigureAwait(false));

    public long LRem(string key, long count, string element) => UseClient(x => x.RemoveItemFromList(key, element, (int)count));

    public Task<long> LRemAsync(string key, long count, string element) => UseClientAsync(async x => await x.RemoveItemFromListAsync(key, element, (int)count).ConfigureAwait(false));

    public void LTrim(string key, long start, long stop) => UseClient(x => x.TrimList(key, (int)start, (int)stop));

    public Task LTrimAsync(string key, long start, long stop) => UseClientAsync(x => x.TrimListAsync(key, (int)start, (int)stop).ConfigureAwait(false));

    public string[] LRange(string key, long start, long stop) => UseClient(x => x.GetRangeFromList(key, (int)start, (int)stop)?.ToArray() ?? []);

    public Task<string[]> LRangeAsync(string key, long start, long stop) => UseClientAsync(async x => (await x.GetRangeFromListAsync(key, (int)start, (int)stop).ConfigureAwait(false))?.ToArray() ?? []);

    public long LLen(string key) => UseClient(x => x.GetListCount(key));

    public Task<long> LLenAsync(string key) => UseClientAsync(async x => await x.GetListCountAsync(key).ConfigureAwait(false));

    public string? LIndex(string key, long index) => UseClient(x => x.GetItemFromList(key, (int)index));

    public Task<string?> LIndexAsync(string key, long index) => UseClientAsync(async x => await x.GetItemFromListAsync(key, (int)index).ConfigureAwait(false));

    public string? RPop(string key) => UseClient(x => x.RemoveEndFromList(key));

    public Task<string?> RPopAsync(string key) => UseClientAsync(async x => await x.RemoveEndFromListAsync(key).ConfigureAwait(false));
    public string? LPop(string key) => UseClient(x => x.RemoveStartFromList(key));

    public Task<string?> LPopAsync(string key) => UseClientAsync(async x => await x.RemoveStartFromListAsync(key).ConfigureAwait(false));

    public long IncrBy(string key, long increment) => UseClient(x => x.IncrementValueBy(key, increment));
    public Task<long> IncrByAsync(string key, long increment) => UseClientAsync(async x => await x.IncrementValueByAsync(key, increment).ConfigureAwait(false));

    public bool LockExtend(string key, string value, TimeSpan expiry)
    {
        return UseClient(x =>
        {
            var luaScript = @"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('EXPIRE', KEYS[1], ARGV[2])
            else
                return 0
            end
        ";

            var result = x.ExecLua(luaScript, [key], [
                value,
                expiry.TotalSeconds.ToString()
            ]);
            return result?.GetResult() == "1";
        });
    }

    public bool LockTake(string key, string value, TimeSpan expiry) => UseClient(x => x.SetValueIfNotExists(key, value, expiry));

    public bool LockRelease(string key, string value)
    {
        return UseClient(x =>
        {
            var luaScript = @"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            else
                return 0
            end
        ";
            var result = x.ExecLua(luaScript, [key], [value]);
            return result?.GetResult() == "1";
        });
    }

    public long? Ttl(string key) => UseClient(x => ((IRedisNativeClient)x).Ttl(key));

    public Task<long?> TtlAsync(string key) => UseClientAsync<long?>(async x => await ((IRedisNativeClientAsync)x).TtlAsync(key).ConfigureAwait(false));

    public bool Exists(string key) => UseClient(x => x.ContainsKey(key));

    public Task<bool> ExistsAsync(string key) => UseClientAsync(async x => await x.ContainsKeyAsync(key).ConfigureAwait(false));

    public DateTime Time()
    {
        return UseClient(x =>
        {
            var result = x.ExecLua("return redis.call('TIME')", [], [])?.GetResults();

            if (result is { Count: 2 } timeResult)
            {
                // 提取秒数和微秒数
                if (long.TryParse(timeResult[0], out var seconds) && long.TryParse(timeResult[1], out var microseconds))
                {
                    // 将秒数转换为 DateTime 对象
                    var redisTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(seconds).AddMicroseconds(microseconds);

                    return redisTime;
                }
            }

            throw new Exception($"get time error: {result}");
        });
    }
}