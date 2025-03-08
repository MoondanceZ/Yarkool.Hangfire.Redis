using ServiceStack.Redis;
using ServiceStack.Redis.Pipeline;

namespace Yarkool.Hangfire.Redis.ServiceStack;

public partial class ServiceStackCommand : IRedisCommand, IDisposable
{
    private readonly global::ServiceStack.Redis.Pipeline.IRedisPipeline? _redisPipeline;
    private readonly global::ServiceStack.Redis.IRedisTransaction? _redisTransaction;
    private global::ServiceStack.Redis.IRedisClient? _redisClient;
    private global::ServiceStack.Redis.IRedisClientAsync? _redisClientAsync;
    private readonly IRedisClientsManager _redisClientsManager;

    private List<object> _commandResults;

    public ServiceStackCommand(IRedisClientsManager redisClientsManager)
    {
        _redisClientsManager = redisClientsManager;
        if (GetType() == typeof(ServiceStackPipeline))
        {
            _redisPipeline = RedisClient.CreatePipeline();
            _commandResults = new List<object>();
        }
        else if (GetType() == typeof(ServiceStackTransaction))
        {
            _redisTransaction = RedisClient.CreateTransaction();
            _commandResults = new List<object>();
        }
    }

    private global::ServiceStack.Redis.IRedisClient RedisClient => _redisClient ??= _redisClientsManager.GetClient();

    private global::ServiceStack.Redis.IRedisClientAsync RedisClientAsync => _redisClientAsync ??= _redisClientsManager.GetClientAsync().ConfigureAwait(false).GetAwaiter().GetResult();

    private IRedisQueueableOperation? RedisQueueableOperation => _redisPipeline as IRedisQueueableOperation ?? _redisTransaction;

    public long Publish(string channel, string message) => RedisClient.PublishMessage(channel, message);

    public async Task<long> PublishAsync(string channel, string message) => await RedisClientAsync.PublishMessageAsync(channel, message).ConfigureAwait(false);

    public IDisposable? Subscribe(string key, Action<string, object> handler) => new RedisPubSubServer(_redisClientsManager, key) { OnMessage = handler }.Start();

    public void UnSubscribe(string key) => new RedisPubSubServer(_redisClientsManager, key).Stop();

    public string? Get(string key)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetValue(key), x => _commandResults.Add(x));
            return null;
        }

        return RedisClient.GetValue(key);
    }

    public async Task<string?> GetAsync(string key)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetValue(key), x => _commandResults.Add(x));
            return null;
        }

        return await RedisClientAsync.GetValueAsync(key).ConfigureAwait(false);
    }

    public string?[] MGet(params string[] keys)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetValues(keys.ToList()), x => _commandResults.Add(x.ToArray()));
            return [];
        }

        return RedisClient.GetValues(keys.ToList()).ToArray();
    }

    public bool Set(string key, string value)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.SetValue(key, value), () => _commandResults.Add(true));
            return false;
        }

        RedisClient.SetValue(key, value);
        return true;
    }

    public async Task<bool> SetAsync(string key, string value)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.SetValue(key, value), () => _commandResults.Add(true));
            return false;
        }

        await RedisClientAsync.SetValueAsync(key, value).ConfigureAwait(false);
        return true;
    }

    public bool Set(string key, string value, TimeSpan expiry)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.SetValue(key, value, expiry), () => _commandResults.Add(true));
            return false;
        }

        RedisClient.SetValue(key, value, expiry);
        return true;
    }

    public async Task<bool> SetAsync(string key, string value, TimeSpan expiry)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.SetValue(key, value, expiry), () => _commandResults.Add(true));
            return false;
        }

        await RedisClientAsync.SetValueAsync(key, value, expiry).ConfigureAwait(false);
        return true;
    }

    public bool SetNx(string key, string value)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.SetValueIfNotExists(key, value), s => _commandResults.Add(s));
            return false;
        }

        return RedisClient.SetValueIfNotExists(key, value);
    }

    public async Task<bool> SetNxAsync(string key, string value)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.SetValueIfNotExists(key, value), s => _commandResults.Add(s));
            return false;
        }

        return await RedisClientAsync.SetValueIfNotExistsAsync(key, value).ConfigureAwait(false);
    }

    public bool SetNx(string key, string value, TimeSpan expiry)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.SetValueIfNotExists(key, value, expiry), s => _commandResults.Add(s));
            return false;
        }

        return RedisClient.SetValueIfNotExists(key, value, expiry);
    }

    public async Task<bool> SetNxAsync(string key, string value, TimeSpan expiry)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.SetValueIfNotExists(key, value, expiry), s => _commandResults.Add(s));
            return false;
        }

        return await RedisClientAsync.SetValueIfNotExistsAsync(key, value, expiry).ConfigureAwait(false);
    }

    public long ZAdd(string key, string member, decimal score)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.AddItemToSortedSet(key, member, (double)score), s => _commandResults.Add(s ? 1L : 0));
            return 0;
        }

        RedisClient.AddItemToSortedSet(key, member, (double)score);
        return 1L;
    }

    public async Task<long> ZAddAsync(string key, string member, decimal score)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.AddItemToSortedSet(key, member, (double)score), s => _commandResults.Add(s ? 1L : 0));
            return 0;
        }

        await RedisClientAsync.AddItemToSortedSetAsync(key, member, (double)score).ConfigureAwait(false);
        return 1L;
    }

    public long ZAdd(string key, Dictionary<string, decimal> data)
    {
        if (RedisQueueableOperation != null)
        {
            foreach (var item in data)
            {
                RedisQueueableOperation.QueueCommand(x => x.AddItemToSortedSet(key, item.Key, (double)item.Value), () => _commandResults.Add(1L));
            }

            return 0;
        }

        var pipeline = RedisClient.CreatePipeline();
        foreach (var item in data)
        {
            pipeline.QueueCommand(s => s.AddItemToSortedSet(key, item.Key, (double)item.Value));
        }

        pipeline.Flush();
        return data.Count;
    }

    public async Task<long> ZAddAsync(string key, Dictionary<string, decimal> data)
    {
        if (RedisQueueableOperation != null)
        {
            foreach (var item in data)
            {
                RedisQueueableOperation.QueueCommand(x => x.AddItemToSortedSet(key, item.Key, (double)item.Value), () => _commandResults.Add(1L));
            }

            return 0;
        }

        var pipeline = RedisClientAsync.CreatePipeline();
        foreach (var item in data)
        {
            pipeline.QueueCommand(async s => await s.AddItemToSortedSetAsync(key, item.Key, (double)item.Value).ConfigureAwait(false));
        }

        await pipeline.FlushAsync();
        return data.LongCount();
    }

    public long ZRem(string key, params string[] members)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.RemoveItemsFromSortedSet(key, members.ToList()), s => _commandResults.Add(s));
            return 0;
        }

        return RedisClient.RemoveItemsFromSortedSet(key, members.ToList());
    }

    public async Task<long> ZRemAsync(string key, params string[] members)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.RemoveItemsFromSortedSet(key, members.ToList()), s => _commandResults.Add(s));
            return 0;
        }

        return await RedisClientAsync.RemoveItemsFromSortedSetAsync(key, members.ToList()).ConfigureAwait(false);
    }

    public long ZCard(string key)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetSortedSetCount(key), s => _commandResults.Add(s));
            return 0;
        }

        return RedisClient.GetSortedSetCount(key);
    }

    public async Task<long> ZCardAsync(string key)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetSortedSetCount(key), s => _commandResults.Add(s));
            return 0;
        }

        return await RedisClientAsync.GetSortedSetCountAsync(key).ConfigureAwait(false);
    }

    public long ZCount(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetSortedSetCount(key, (double)min, (double)max), s => _commandResults.Add(s));
            return 0;
        }

        return RedisClient.GetSortedSetCount(key, (double)min, (double)max);
    }

    public async Task<long> ZCountAsync(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetSortedSetCount(key, (double)min, (double)max), s => _commandResults.Add(s));
            return 0;
        }

        return await RedisClientAsync.GetSortedSetCountAsync(key, (double)min, (double)max).ConfigureAwait(false);
    }

    public IRedisCommand.ScanResult<IRedisCommand.ZMember> ZScan(string key, long cursor, string? pattern = null, long count = 0)
    {
        if (RedisQueueableOperation != null)
            throw new NotSupportedException();

        var result = ((IRedisNativeClient)RedisClient).ZScan(key, (ulong)cursor, (int)count, pattern);
        return new IRedisCommand.ScanResult<IRedisCommand.ZMember>((long)result.Cursor, result.AsItemsWithScores()?.Select(s => new IRedisCommand.ZMember(s.Key, (decimal)s.Value)).ToArray() ?? []);
    }

    public string[] ZRangeByScore(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue, int skip = 0, int take = 0)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetRangeFromSortedSetByLowestScore(key, (double)min, (double)max, skip, take), s => _commandResults.Add(s?.ToArray() ?? []));
            return [];
        }

        return RedisClient.GetRangeFromSortedSetByLowestScore(key, (double)min, (double)max, skip, take)?.ToArray() ?? [];
    }

    public async Task<string[]> ZRangeByScoreAsync(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue, int skip = 0, int take = 0)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetRangeFromSortedSetByLowestScore(key, (double)min, (double)max, skip, take), s => _commandResults.Add(s?.ToArray() ?? []));
            return [];
        }

        return (await RedisClientAsync.GetRangeFromSortedSetByLowestScoreAsync(key, (double)min, (double)max, skip, take).ConfigureAwait(false))?.ToArray() ?? [];
    }

    public string[] ZRangeByRank(string key, long start, long stop)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetRangeFromSortedSet(key, (int)start, (int)stop), s => _commandResults.Add(s?.ToArray() ?? []));
            return [];
        }

        return RedisClient.GetRangeFromSortedSet(key, (int)start, (int)stop)?.ToArray() ?? [];
    }

    public async Task<string[]> ZRangeByRankAsync(string key, long start, long stop)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetRangeFromSortedSet(key, (int)start, (int)stop), s => _commandResults.Add(s?.ToArray() ?? []));
            return [];
        }

        return (await RedisClientAsync.GetRangeFromSortedSetAsync(key, (int)start, (int)stop).ConfigureAwait(false))?.ToArray() ?? [];
    }

    public string[] ZRevRangeByRank(string key, long start, long stop)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetRangeFromSortedSetDesc(key, (int)start, (int)stop), s => _commandResults.Add(s?.ToArray() ?? []));
            return [];
        }

        return RedisClient.GetRangeFromSortedSetDesc(key, (int)start, (int)stop)?.ToArray() ?? [];
    }

    public async Task<string[]> ZRevRangeByRankAsync(string key, long start, long stop)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetRangeFromSortedSetDesc(key, (int)start, (int)stop), s => _commandResults.Add(s?.ToArray() ?? []));
            return [];
        }

        return (await RedisClientAsync.GetRangeFromSortedSetDescAsync(key, (int)start, (int)stop).ConfigureAwait(false))?.ToArray() ?? [];
    }

    public (string Element, decimal Score)[] ZRangeByRankWithScores(string key, long start, long stop)
    {
        if (RedisQueueableOperation != null)
            throw new NotSupportedException();

        return RedisClient.GetRangeWithScoresFromSortedSet(key, (int)start, (int)stop)?.Select(s => (s.Key, (decimal)s.Value)).ToArray() ?? [];
    }

    public async Task<(string Element, decimal Score)[]> ZRangeByRankWithScoresAsync(string key, long start, long stop)
    {
        if (RedisQueueableOperation != null)
            throw new NotSupportedException();

        return (await RedisClientAsync.GetRangeWithScoresFromSortedSetAsync(key, (int)start, (int)stop).ConfigureAwait(false))?.Select(s => (s.Key, (decimal)s.Value)).ToArray() ?? [];
    }

    public (string Element, decimal Score)[] ZRangeByScoreWithScores(string key, decimal min, decimal max, long offset = 0, long count = 0)
    {
        if (RedisQueueableOperation != null)
            throw new NotSupportedException();

        return RedisClient.GetRangeWithScoresFromSortedSetByLowestScore(key, (double)min, (double)max, (int)offset, (int)count)?.Select(s => (s.Key, (decimal)s.Value)).ToArray() ?? [];
    }

    public async Task<(string Element, decimal Score)[]> ZRangeByScoreWithScoresAsync(string key, decimal min, decimal max, long offset = 0, long count = 0)
    {
        if (RedisQueueableOperation != null)
            throw new NotSupportedException();

        return (await RedisClientAsync.GetRangeWithScoresFromSortedSetByLowestScoreAsync(key, (double)min, (double)max, (int)offset, (int)count).ConfigureAwait(false))?.Select(s => (s.Key, (decimal)s.Value)).ToArray() ?? [];
    }

    public long? ZRank(string key, string member)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetItemIndexInSortedSet(key, member), s => _commandResults.Add(s));
            return 0;
        }

        return RedisClient.GetItemIndexInSortedSet(key, member);
    }

    public async Task<long?> ZRankAsync(string key, string member)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetItemIndexInSortedSet(key, member), s => _commandResults.Add(s));
            return 0;
        }

        return await RedisClientAsync.GetItemIndexInSortedSetAsync(key, member).ConfigureAwait(false);
    }

    public decimal? ZScore(string key, string member)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetItemScoreInSortedSet(key, member), s => _commandResults.Add(s));
            return 0;
        }

        return (decimal)RedisClient.GetItemScoreInSortedSet(key, member);
    }

    public async Task<decimal?> ZScoreAsync(string key, string member)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetItemScoreInSortedSet(key, member), s => _commandResults.Add(s));
            return 0;
        }

        return (decimal?)await RedisClientAsync.GetItemScoreInSortedSetAsync(key, member).ConfigureAwait(false);
    }

    public long SAdd(string key, params string[] members)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.AddRangeToSet(key, members.ToList()), () => _commandResults.Add(1));
            return 0;
        }

        RedisClient.AddRangeToSet(key, members.ToList());
        return members.Length;
    }

    public async Task<long> SAddAsync(string key, params string[] members)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.AddRangeToSet(key, members.ToList()), () => _commandResults.Add(1));
            return 0;
        }

        await RedisClientAsync.AddRangeToSetAsync(key, members.ToList()).ConfigureAwait(false);
        return members.Length;
    }

    public long SRem(string key, string member)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.RemoveItemFromSet(key, member), () => _commandResults.Add(1));
            return 0;
        }

        RedisClient.RemoveItemFromSet(key, member);
        return 1L;
    }

    public async Task<long> SRemAsync(string key, string member)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.RemoveItemFromSet(key, member), () => _commandResults.Add(1));
            return 0;
        }

        await RedisClientAsync.RemoveItemFromSetAsync(key, member).ConfigureAwait(false);
        return 1L;
    }

    public string[] SMembers(string key)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetAllItemsFromSet(key), s => _commandResults.Add(s.ToArray()));
            return [];
        }

        return RedisClient.GetAllItemsFromSet(key)?.ToArray() ?? [];
    }

    public async Task<string[]> SMembersAsync(string key)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetAllItemsFromSet(key), s => _commandResults.Add(s.ToArray()));
            return [];
        }

        return (await RedisClientAsync.GetAllItemsFromSetAsync(key).ConfigureAwait(false))?.ToArray() ?? [];
    }

    public bool SIsMember(string key, string member)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.SetContainsItem(key, member), s => _commandResults.Add(s));
            return false;
        }

        return RedisClient.SetContainsItem(key, member);
    }

    public async Task<bool> SIsMemberAsync(string key, string member)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.SetContainsItem(key, member), s => _commandResults.Add(s));
            return false;
        }

        return await RedisClientAsync.SetContainsItemAsync(key, member).ConfigureAwait(false);
    }

    public long SCard(string key)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetSetCount(key), s => _commandResults.Add(s));
            return 0;
        }

        return RedisClient.GetSetCount(key);
    }

    public async Task<long> SCardAsync(string key)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetSetCount(key), s => _commandResults.Add(s));
            return 0;
        }

        return await RedisClientAsync.GetSetCountAsync(key).ConfigureAwait(false);
    }

    public bool Expire(string key, TimeSpan expiry)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.ExpireEntryIn(key, expiry), s => _commandResults.Add(s));
            return false;
        }

        return RedisClient.ExpireEntryIn(key, expiry);
    }

    public async Task<bool> ExpireAsync(string key, TimeSpan expiry)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.ExpireEntryIn(key, expiry), s => _commandResults.Add(s));
            return false;
        }

        return await RedisClientAsync.ExpireEntryInAsync(key, expiry).ConfigureAwait(false);
    }

    public bool Persist(string key) => ((IRedisNativeClient)RedisClient).Persist(key);

    public async Task<bool> PersistAsync(string key) => await ((IRedisNativeClientAsync)RedisClientAsync).PersistAsync(key).ConfigureAwait(false);

    public long Del(params string[] keys)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.RemoveAll(keys), () => _commandResults.Add(keys.Length));
            return 0;
        }

        RedisClient.RemoveAll(keys);
        return keys.Length;
    }

    public async Task<long> DelAsync(params string[] keys)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.RemoveAll(keys), () => _commandResults.Add(keys.Length));
            return 0;
        }

        await RedisClientAsync.RemoveAllAsync(keys).ConfigureAwait(false);
        return keys.Length;
    }

    public long HSet(string key, string field, string value)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.SetEntryInHash(key, field, value), s => _commandResults.Add(s ? 1 : 0));
            return 0;
        }

        RedisClient.SetEntryInHash(key, field, value);
        return 1L;
    }

    public async Task<long> HSetAsync(string key, string field, string value)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.SetEntryInHash(key, field, value), s => _commandResults.Add(s ? 1 : 0));
            return 0;
        }

        await RedisClientAsync.SetEntryInHashAsync(key, field, value).ConfigureAwait(false);
        return 1L;
    }

    public long HSet(string key, Dictionary<string, string> keyValues)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.SetRangeInHash(key, keyValues), () => _commandResults.Add(keyValues.Count));
            return 0;
        }

        RedisClient.SetRangeInHash(key, keyValues);
        return keyValues.LongCount();
    }

    public async Task<long> HSetAsync(string key, Dictionary<string, string> keyValues)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.SetRangeInHash(key, keyValues), () => _commandResults.Add(keyValues.LongCount()));
            return 0;
        }

        await RedisClientAsync.SetRangeInHashAsync(key, keyValues).ConfigureAwait(false);
        return keyValues.LongCount();
    }

    public string? HGet(string key, string field)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetValueFromHash(key, field), s => _commandResults.Add(s));
            return null;
        }

        return RedisClient.GetValueFromHash(key, field);
    }

    public async Task<string?> HGetAsync(string key, string field)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetValueFromHash(key, field), s => _commandResults.Add(s));
            return null;
        }

        return await RedisClientAsync.GetValueFromHashAsync(key, field).ConfigureAwait(false);
    }

    public string?[]? HMGet(string key, params string[] fields)
    {
        if (RedisQueueableOperation != null)
        {
            RedisQueueableOperation.QueueCommand(x => x.GetValuesFromHash(key, fields), s =>
            {
                var resultCount = s.Count;
                if (resultCount < fields.Length)
                {
                    for (var i = 0; i < fields.Length - resultCount; i++)
                    {
                        s.Add(null);
                    }
                }

                _commandResults.Add(s);
            });
            return [];
        }
        
        var result = RedisClient.GetValuesFromHash(key, fields) ?? [];
        var resultCount = result.Count;
        if (resultCount < fields.Length)
        {
            for (var i = 0; i < fields.Length - resultCount; i++)
            {
                result.Add(null);
            }
        }

        return result.ToArray();
    }

    public async Task<string?[]?> HMGetAsync(string key, params string[] fields)
    {
        #pragma warning disable CS8619
        var result = await RedisClientAsync.GetValuesFromHashAsync(key, fields).ConfigureAwait(false) ?? [];
        var resultCount = result.Count;
        if (resultCount < fields.Length)
        {
            for (var i = 0; i < fields.Length - resultCount; i++)
            {
                result.Add(null);
            }
        }

        return result.Select(s => s).ToArray();
    }

    public Dictionary<string, string> HGetAll(string key)
    {
        return RedisClient.GetAllEntriesFromHash(key);
    }

    public long HLen(string key)
    {
        return RedisClient.GetHashCount(key);
    }

    public async Task<long> HLenAsync(string key)
    {
        return await RedisClientAsync.GetHashCountAsync(key).ConfigureAwait(false);
    }

    public long HDel(string key, params string[] fields)
    {
        var pipeline = RedisClient.CreatePipeline();
        foreach (var field in fields)
        {
            pipeline.QueueCommand(s => s.RemoveEntryFromHash(key, field));
        }

        pipeline.Flush();
        return fields.LongLength;
    }

    public async Task<long> HDelAsync(string key, params string[] fields)
    {
        var pipeline = RedisClientAsync.CreatePipeline();
        foreach (var field in fields)
        {
            pipeline.QueueCommand(async s => await s.RemoveEntryFromHashAsync(key, field).ConfigureAwait(false));
        }

        await pipeline.FlushAsync();
        return fields.LongLength;
    }

    public bool HExists(string key, string field)
    {
        return RedisClient.HashContainsEntry(key, field);
    }

    public async Task<bool> HExistsAsync(string key, string field)
    {
        return await RedisClientAsync.HashContainsEntryAsync(key, field).ConfigureAwait(false);
    }

    public long RPush(string key, params string[] elements)
    {
        RedisClient.AddRangeToList(key, elements.ToList());
        return elements.LongLength;
    }

    public async Task<long> RPushAsync(string key, params string[] elements)
    {
        await RedisClientAsync.AddRangeToListAsync(key, elements.ToList()).ConfigureAwait(false);
        return elements.LongLength;
    }

    public long LPush(string key, params string[] elements)
    {
        RedisClient.PrependRangeToList(key, elements.ToList());
        return elements.LongLength;
    }

    public async Task<long> LPushAsync(string key, params string[] elements)
    {
        await RedisClientAsync.PrependRangeToListAsync(key, elements.ToList()).ConfigureAwait(false);
        return elements.LongLength;
    }

    public string? RPopLPush(string source, string destination)
    {
        return RedisClient.PopAndPushItemBetweenLists(source, destination);
    }

    public async Task<string?> RPopLPushAsync(string source, string destination)
    {
        return await RedisClientAsync.PopAndPushItemBetweenListsAsync(source, destination).ConfigureAwait(false);
    }

    public long LRem(string key, long count, string element)
    {
        return RedisClient.RemoveItemFromList(key, element, (int)count);
    }

    public async Task<long> LRemAsync(string key, long count, string element)
    {
        return await RedisClientAsync.RemoveItemFromListAsync(key, element, (int)count).ConfigureAwait(false);
    }

    public void LTrim(string key, long start, long stop)
    {
        RedisClient.TrimList(key, (int)start, (int)stop);
    }

    public async Task LTrimAsync(string key, long start, long stop)
    {
        await RedisClientAsync.TrimListAsync(key, (int)start, (int)stop).ConfigureAwait(false);
    }

    public string[] LRange(string key, long start, long stop)
    {
        return RedisClient.GetRangeFromList(key, (int)start, (int)stop)?.ToArray() ?? [];
    }

    public async Task<string[]> LRangeAsync(string key, long start, long stop)
    {
        return (await RedisClientAsync.GetRangeFromListAsync(key, (int)start, (int)stop).ConfigureAwait(false))?.ToArray() ?? [];
    }

    public long LLen(string key)
    {
        return RedisClient.GetListCount(key);
    }

    public async Task<long> LLenAsync(string key)
    {
        return await RedisClientAsync.GetListCountAsync(key).ConfigureAwait(false);
    }

    public string? LIndex(string key, long index)
    {
        return RedisClient.GetItemFromList(key, (int)index);
    }

    public async Task<string?> LIndexAsync(string key, long index)
    {
        return await RedisClientAsync.GetItemFromListAsync(key, (int)index).ConfigureAwait(false);
    }

    public string? RPop(string key)
    {
        return RedisClient.RemoveEndFromList(key);
    }

    public async Task<string?> RPopAsync(string key)
    {
        return await RedisClientAsync.RemoveEndFromListAsync(key).ConfigureAwait(false);
    }

    public string? LPop(string key)
    {
        return RedisClient.RemoveStartFromList(key);
    }

    public async Task<string?> LPopAsync(string key)
    {
        return await RedisClientAsync.RemoveStartFromListAsync(key).ConfigureAwait(false);
    }

    public long IncrBy(string key, long increment)
    {
        return RedisClient.IncrementValueBy(key, increment);
    }

    public async Task<long> IncrByAsync(string key, long increment)
    {
        return await RedisClientAsync.IncrementValueByAsync(key, increment).ConfigureAwait(false);
    }

    public bool LockExtend(string key, string value, TimeSpan expiry)
    {
        var luaScript = @"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('EXPIRE', KEYS[1], ARGV[2])
            else
                return 0
            end
        ";

        var result = RedisClient.ExecLua(luaScript, [key], [
            value,
            expiry.TotalSeconds.ToString()
        ]);
        return result?.GetResult() == "1";
    }

    public bool LockTake(string key, string value, TimeSpan expiry)
    {
        return RedisClient.SetValueIfNotExists(key, value, expiry);
    }

    public bool LockRelease(string key, string value)
    {
        var luaScript = @"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            else
                return 0
            end
        ";
        var result = RedisClient.ExecLua(luaScript, [key], [value]);
        return result?.GetResult() == "1";
    }

    public long? Ttl(string key)
    {
        return ((IRedisNativeClient)RedisClient).Ttl(key);
    }

    public async Task<long?> TtlAsync(string key)
    {
        return await ((IRedisNativeClientAsync)RedisClientAsync).TtlAsync(key).ConfigureAwait(false);
    }

    public bool Exists(string key)
    {
        return RedisClient.ContainsKey(key);
    }

    public async Task<bool> ExistsAsync(string key)
    {
        return await RedisClientAsync.ContainsKeyAsync(key).ConfigureAwait(false);
    }

    public DateTime Time()
    {
        var result = RedisClient.ExecLua("return redis.call('TIME')", keys: [], args: [])?.GetResults();

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
    }

    public void Dispose()
    {
        _redisPipeline?.Dispose();
        _redisTransaction?.Dispose();
        _redisClient?.Dispose();
        if (_redisClientAsync is IDisposable redisClientAsyncDisposable)
            redisClientAsyncDisposable.Dispose();
        else if (_redisClientAsync != null)

            _ = _redisClientAsync.DisposeAsync().AsTask();
        _redisClientsManager.Dispose();
    }
}