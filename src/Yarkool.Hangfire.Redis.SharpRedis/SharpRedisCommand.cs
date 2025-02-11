using SharpRedis.Provider.Standard;

namespace Yarkool.Hangfire.Redis.SharpRedis;

public partial class SharpRedisCommand(BaseRedis redisClient) : IRedisCommand
{
    public long Publish(string channel, string message) => redisClient.PubSub.Publish(channel, message);

    public Task<long> PublishAsync(string channel, string message) => redisClient.PubSub.PublishAsync(channel, message);

    public IDisposable? Subscribe(string key, Action<string, object> handler)
    {
        redisClient.PubSub.Subscribe(key, (channel, data) =>
        {
            handler(channel, data);
            return Task.CompletedTask;
        });
        return null;
    }

    public void UnSubscribe(string key) => redisClient.PubSub.UnSubscribe(key);

    public string? Get(string key) => redisClient.String.Get(key);

    public Task<string?> GetAsync(string key) => redisClient.String.GetAsync(key);

    public string?[] MGet(params string[] keys) => redisClient.String.MGet(keys);

    public bool Set(string key, string value) => redisClient.String.Set(key, value);

    public Task<bool> SetAsync(string key, string value) => redisClient.String.SetAsync(key, value);

    public bool Set(string key, string value, TimeSpan expiry) => redisClient.String.Set(key, value, expiry);

    public Task<bool> SetAsync(string key, string value, TimeSpan expiry) => redisClient.String.SetAsync(key, value, expiry);

    public bool SetNx(string key, string value) => redisClient.String.SetNx(key, value);

    public Task<bool> SetNxAsync(string key, string value) => redisClient.String.SetNxAsync(key, value);

    public bool SetNx(string key, string value, TimeSpan expiry) => redisClient.String.SetNx(key, value, expiry);

    public Task<bool> SetNxAsync(string key, string value, TimeSpan expiry) => redisClient.String.SetNxAsync(key, value, expiry);

    public long ZAdd(string key, string member, decimal score) => redisClient.SortedSet.ZAdd(key, member, score);

    public Task<long> ZAddAsync(string key, string member, decimal score) => redisClient.SortedSet.ZAddAsync(key, member, score);

    public long ZAdd(string key, Dictionary<string, decimal> data) => redisClient.SortedSet.ZAdd(key, data);

    public Task<long> ZAddAsync(string key, Dictionary<string, decimal> data) => redisClient.SortedSet.ZAddAsync(key, data);

    public long ZRem(string key, params string[] members) => redisClient.SortedSet.ZRem(key, members);

    public Task<long> ZRemAsync(string key, params string[] members) => redisClient.SortedSet.ZRemAsync(key, members);

    public long ZCard(string key) => redisClient.SortedSet.ZCard(key);

    public Task<long> ZCardAsync(string key) => redisClient.SortedSet.ZCardAsync(key);

    public long ZCount(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue) => redisClient.SortedSet.ZCount(key, min, max);

    public Task<long> ZCountAsync(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue) => redisClient.SortedSet.ZCountAsync(key, min, max);

    public IRedisCommand.ScanResult<IRedisCommand.ZMember> ZScan(string key, long cursor, string? pattern = null, long count = 0)
    {
        var result = redisClient.SortedSet.ZScan(key, cursor, pattern, (ulong)count)!;
        return new IRedisCommand.ScanResult<IRedisCommand.ZMember>((long)result.Cursor, result.Data?.Select(x => new IRedisCommand.ZMember(x.Member, x.Score)).ToArray() ?? []);
    }

    public string[] ZRangeByScore(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue, int skip = 0, int take = 0) => redisClient.SortedSet.ZRangeByScore(key, min, max, (ulong)skip, (ulong)take) ?? [];

    public async Task<string[]> ZRangeByScoreAsync(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue, int skip = 0, int take = 0) => (await redisClient.SortedSet.ZRangeByScoreAsync(key, min, max, (ulong)skip, (ulong)take)) ?? [];

    public string[] ZRangeByRank(string key, long start, long stop) => redisClient.SortedSet.ZRange(key, start, stop) ?? [];

    public async Task<string[]> ZRangeByRankAsync(string key, long start, long stop) => ((await redisClient.SortedSet.ZRangeAsync(key, start, stop)) ?? []);

    public string[] ZRevRangeByRank(string key, long start, long stop) => redisClient.SortedSet.ZRevRange(key, start, stop) ?? [];

    public async Task<string[]> ZRevRangeByRankAsync(string key, long start, long stop) => ((await redisClient.SortedSet.ZRevRangeAsync(key, start, stop)) ?? []);

    public (string Element, decimal Score)[] ZRangeByRankWithScores(string key, long start, long stop) => redisClient.SortedSet.ZRangeWithScores(key, start, stop)?.Select(x => (x.Member, x.Score.ToDecimal())).ToArray() ?? [];

    public async Task<(string Element, decimal Score)[]> ZRangeByRankWithScoresAsync(string key, long start, long stop) => (await redisClient.SortedSet.ZRangeWithScoresAsync(key, start, stop))?.Select(x => (x.Member, x.Score.ToDecimal())).ToArray() ?? [];

    public (string Element, decimal Score)[] ZRangeByScoreWithScores(string key, decimal min, decimal max, long offset = 0, long count = 0) => redisClient.SortedSet.ZRangeByScoreWithScores(key, min, max, (ulong)offset, (ulong)count)?.Select(x => (x.Member, x.Score.ToDecimal())).ToArray() ?? [];

    public async Task<(string Element, decimal Score)[]> ZRangeByScoreWithScoresAsync(string key, decimal min, decimal max, long offset = 0, long count = 0) => (await redisClient.SortedSet.ZRangeByScoreWithScoresAsync(key, min, max, (ulong)offset, (ulong)count))?.Select(x => (x.Member, x.Score.ToDecimal())).ToArray() ?? [];

    public long? ZRank(string key, string member) => redisClient.SortedSet.ZRank(key, member);

    public Task<long?> ZRankAsync(string key, string member) => redisClient.SortedSet.ZRankAsync(key, member);

    public decimal? ZScore(string key, string member) => redisClient.SortedSet.ZScore(key, member)?.ToDecimal();

    public async Task<decimal?> ZScoreAsync(string key, string member) => (await redisClient.SortedSet.ZScoreAsync(key, member))?.ToDecimal();

    public long SAdd(string key, params string[] members) => redisClient.Set.SAdd(key, members);

    public Task<long> SAddAsync(string key, params string[] members) => redisClient.Set.SAddAsync(key, members);

    public long SRem(string key, string member) => redisClient.Set.SRem(key, member);

    public Task<long> SRemAsync(string key, string member) => redisClient.Set.SRemAsync(key, member);

    public string[] SMembers(string key) => redisClient.Set.SMembers(key) ?? [];

    public async Task<string[]> SMembersAsync(string key) => (await redisClient.Set.SMembersAsync(key)) ?? [];

    public bool SIsMember(string key, string member) => redisClient.Set.SIsMember(key, member);

    public Task<bool> SIsMemberAsync(string key, string member) => redisClient.Set.SIsMemberAsync(key, member);

    public long SCard(string key) => redisClient.Set.SCard(key);

    public Task<long> SCardAsync(string key) => redisClient.Set.SCardAsync(key);

    public bool Expire(string key, TimeSpan expiry) => redisClient.Key.Expire(key, expiry);

    public Task<bool> ExpireAsync(string key, TimeSpan expiry) => redisClient.Key.ExpireAsync(key, expiry);

    public bool Persist(string key) => redisClient.Key.Persist(key);

    public Task<bool> PersistAsync(string key) => redisClient.Key.PersistAsync(key);

    public long Del(params string[] keys) => redisClient.Key.Del(keys);

    public Task<long> DelAsync(params string[] keys) => redisClient.Key.DelAsync(keys);

    public long HSet(string key, string field, string value) => redisClient.Hash.HSet(key, field, value);

    public Task<long> HSetAsync(string key, string field, string value) => redisClient.Hash.HSetAsync(key, field, value);

    public long HSet(string key, Dictionary<string, string> keyValues) => redisClient.Hash.HSet(key, keyValues);

    public Task<long> HSetAsync(string key, Dictionary<string, string> keyValues) => redisClient.Hash.HSetAsync(key, keyValues);

    public string? HGet(string key, string field) => redisClient.Hash.HGet(key, field);

    public Task<string?> HGetAsync(string key, string field) => redisClient.Hash.HGetAsync(key, field);

    public string?[]? HMGet(string key, params string[] fields) => redisClient.Hash.HMGet(key, fields);

    public Task<string?[]?> HMGetAsync(string key, params string[] fields) => redisClient.Hash.HMGetAsync(key, fields);

    public Dictionary<string, string> HGetAll(string key) => redisClient.Hash.HGetAll(key) ?? new Dictionary<string, string>();

    public long HLen(string key) => redisClient.Hash.HLen(key);

    public Task<long> HLenAsync(string key) => redisClient.Hash.HLenAsync(key);

    public long HDel(string key, params string[] fields) => redisClient.Hash.HDel(key, fields);

    public Task<long> HDelAsync(string key, params string[] fields) => redisClient.Hash.HDelAsync(key, fields);

    public bool HExists(string key, string field) => redisClient.Hash.HExists(key, field);

    public Task<bool> HExistsAsync(string key, string field) => redisClient.Hash.HExistsAsync(key, field);

    public long RPush(string key, params string[] elements) => redisClient.List.RPush(key, elements);

    public Task<long> RPushAsync(string key, params string[] elements) => redisClient.List.RPushAsync(key, elements);

    public long LPush(string key, params string[] elements) => redisClient.List.LPush(key, elements);

    public Task<long> LPushAsync(string key, params string[] elements) => redisClient.List.LPushAsync(key, elements);

    public string? RPopLPush(string source, string destination) => redisClient.List.RPopLPush(source, destination);

    public Task<string?> RPopLPushAsync(string source, string destination) => redisClient.List.RPopLPushAsync(source, destination);

    public long LRem(string key, long count, string element) => redisClient.List.LRem(key, count, element);

    public Task<long> LRemAsync(string key, long count, string element) => redisClient.List.LRemAsync(key, count, element);

    public void LTrim(string key, long start, long stop) => redisClient.List.LTrim(key, start, stop);

    public Task LTrimAsync(string key, long start, long stop) => redisClient.List.LTrimAsync(key, start, stop);

    public string[] LRange(string key, long start, long stop) => redisClient.List.LRange(key, start, stop) ?? [];

    public async Task<string[]> LRangeAsync(string key, long start, long stop) => (await redisClient.List.LRangeAsync(key, start, stop)) ?? [];

    public long LLen(string key) => redisClient.List.LLen(key);

    public Task<long> LLenAsync(string key) => redisClient.List.LLenAsync(key);

    public string? LIndex(string key, long index) => redisClient.List.LIndex(key, index);

    public Task<string?> LIndexAsync(string key, long index) => redisClient.List.LIndexAsync(key, index);

    public string? RPop(string key) => redisClient.List.RPop(key);

    public Task<string?> RPopAsync(string key) => redisClient.List.RPopAsync(key);

    public string? LPop(string key) => redisClient.List.LPop(key);

    public Task<string?> LPopAsync(string key) => redisClient.List.LPopAsync(key);

    public long IncrBy(string key, long increment)
    {
        var result = redisClient.String.IncrBy(key, increment);
        return result.HasValue ? result.ToInt64() : 0;
    }

    public async Task<long> IncrByAsync(string key, long increment)
    {
        var result = await redisClient.String.IncrByAsync(key, increment);
        return result.HasValue ? result.ToInt64() : 0;
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

        var result = redisClient.Script.Eval(luaScript, [key], [
            value,
            expiry.TotalSeconds.ToString()
        ]);
        return result?.ToString() == "1";
    }

    public bool LockTake(string key, string value, TimeSpan expiry) => redisClient.String.SetNx(key, value, expiry);

    public bool LockRelease(string key, string value)
    {
        var luaScript = @"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            else
                return 0
            end
        ";
        var result = redisClient.Script.Eval(luaScript, [key], [value]);
        return result?.ToString() == "1";
    }

    public long? Ttl(string key) => redisClient.Key.Ttl(key);

    public async Task<long?> TtlAsync(string key) => (await redisClient.Key.TtlAsync(key));

    public bool Exists(string key) => redisClient.Key.Exists(key);

    public Task<bool> ExistsAsync(string key) => redisClient.Key.ExistsAsync(key);

    public DateTime Time()
    {
        var result = redisClient.Script.Eval("return redis.call('TIME')", null, null);
        if (result is object[] { Length: 2 } timeResult)
        {
            // 提取秒数和微秒数
            if (long.TryParse(timeResult[0].ToString(), out var seconds) && long.TryParse(timeResult[1].ToString(), out var microseconds))
            {
                // 将秒数转换为 DateTime 对象
                var redisTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(seconds).AddMicroseconds(microseconds);

                return redisTime;
            }
        }

        throw new Exception($"get time error: {result}");
    }
}