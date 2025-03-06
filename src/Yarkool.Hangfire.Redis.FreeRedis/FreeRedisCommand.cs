using FreeRedis;

namespace Yarkool.Hangfire.Redis.FreeRedis;

public partial class FreeRedisCommand(RedisClient redisClient) : IRedisCommand
{
    public long Publish(string channel, string message) => redisClient.Publish(channel, message);

    public Task<long> PublishAsync(string channel, string message) => redisClient.PublishAsync(channel, message);

    public IDisposable? Subscribe(string key, Action<string, object> handler) => redisClient.Subscribe(key, handler);

    public void UnSubscribe(string key) => redisClient.UnSubscribe(key);

    public string? Get(string key) => redisClient.Get(key);

    public Task<string?> GetAsync(string key) => redisClient.GetAsync(key);

    public string?[] MGet(params string[] keys) => redisClient.MGet(keys);

    public bool Set(string key, string value)
    {
        redisClient.Set(key, value);
        return true;
    }

    public async Task<bool> SetAsync(string key, string value)
    {
        await redisClient.SetAsync(key, value);
        return true;
    }

    public bool Set(string key, string value, TimeSpan expiry)
    {
        redisClient.Set(key, value, expiry);
        return true;
    }

    public async Task<bool> SetAsync(string key, string value, TimeSpan expiry)
    {
        await redisClient.SetAsync(key, value, expiry);
        return true;
    }

    public bool SetNx(string key, string value) => redisClient.SetNx(key, value);

    public Task<bool> SetNxAsync(string key, string value) => redisClient.SetNxAsync(key, value);

    public bool SetNx(string key, string value, TimeSpan expiry) => redisClient.SetNx(key, value, expiry);

    public Task<bool> SetNxAsync(string key, string value, TimeSpan expiry) => redisClient.SetNxAsync(key, value, expiry);

    public long ZAdd(string key, string member, decimal score) => redisClient.ZAdd(key, score, member);

    public Task<long> ZAddAsync(string key, string member, decimal score) => redisClient.ZAddAsync(key, score, member);

    public long ZAdd(string key, Dictionary<string, decimal> data) => redisClient.ZAdd(key, data.Select(x => new ZMember(x.Key, x.Value)).ToArray());

    public Task<long> ZAddAsync(string key, Dictionary<string, decimal> data) => redisClient.ZAddAsync(key, data.Select(x => new ZMember(x.Key, x.Value)).ToArray());

    public long ZRem(string key, params string[] members) => redisClient.ZRem(key, members);

    public Task<long> ZRemAsync(string key, params string[] members) => redisClient.ZRemAsync(key, members);

    public long ZCard(string key) => redisClient.ZCard(key);

    public Task<long> ZCardAsync(string key) => redisClient.ZCardAsync(key);

    public long ZCount(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue) => redisClient.ZCount(key, min, max);

    public Task<long> ZCountAsync(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue) => redisClient.ZCountAsync(key, min, max);

    public IRedisCommand.ScanResult<IRedisCommand.ZMember> ZScan(string key, long cursor, string? pattern = null, long count = 0)
    {
        var result = redisClient.ZScan(key, cursor, pattern, count)!;
        return new IRedisCommand.ScanResult<IRedisCommand.ZMember>(result.cursor, result.items?.Select(x => new IRedisCommand.ZMember(x.member, x.score)).ToArray() ?? []);
    }

    public string[] ZRangeByScore(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue, int skip = 0, int take = 0) => redisClient.ZRangeByScore(key, min, max, skip, take) ?? [];

    public async Task<string[]> ZRangeByScoreAsync(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue, int skip = 0, int take = 0) => (await redisClient.ZRangeByScoreAsync(key, min, max, skip, take)) ?? [];

    public string[] ZRangeByRank(string key, long start, long stop) => redisClient.ZRange(key, start, stop) ?? [];

    public async Task<string[]> ZRangeByRankAsync(string key, long start, long stop) => (await redisClient.ZRangeAsync(key, start, stop)) ?? [];

    public string[] ZRevRangeByRank(string key, long start, long stop) => redisClient.ZRevRange(key, start, stop);

    public async Task<string[]> ZRevRangeByRankAsync(string key, long start, long stop) => (await redisClient.ZRevRangeAsync(key, start, stop)) ?? [];

    public (string Element, decimal Score)[] ZRangeByRankWithScores(string key, long start, long stop) => redisClient.ZRangeWithScores(key, start, stop)?.Select(x => (x.member, x.score)).ToArray() ?? [];

    public async Task<(string Element, decimal Score)[]> ZRangeByRankWithScoresAsync(string key, long start, long stop) => (await redisClient.ZRangeWithScoresAsync(key, start, stop))?.Select(x => (x.member, x.score)).ToArray() ?? [];

    public (string Element, decimal Score)[] ZRangeByScoreWithScores(string key, decimal min, decimal max, long offset = 0, long count = 0) => redisClient.ZRangeByScoreWithScores(key, min, max, (int)offset, (int)count)?.Select(x => (x.member, x.score)).ToArray() ?? [];

    public async Task<(string Element, decimal Score)[]> ZRangeByScoreWithScoresAsync(string key, decimal min, decimal max, long offset = 0, long count = 0) => (await redisClient.ZRangeByScoreWithScoresAsync(key, min, max, (int)offset, (int)count))?.Select(x => (x.member, x.score)).ToArray() ?? [];

    public long? ZRank(string key, string member) => redisClient.ZRank(key, member);

    public Task<long?> ZRankAsync(string key, string member) => redisClient.ZRankAsync(key, member);

    public decimal? ZScore(string key, string member) => redisClient.ZScore(key, member);

    public Task<decimal?> ZScoreAsync(string key, string member) => redisClient.ZScoreAsync(key, member);

    public long SAdd(string key, params string[] members) => redisClient.SAdd(key, members.Select(object (x) => x).ToArray());

    public Task<long> SAddAsync(string key, params string[] members) => redisClient.SAddAsync(key, members.Select(object (x) => x).ToArray());

    public long SRem(string key, string member) => redisClient.SRem(key, member);

    public Task<long> SRemAsync(string key, string member) => redisClient.SRemAsync(key, member);

    public string[] SMembers(string key) => redisClient.SMembers(key);

    public Task<string[]> SMembersAsync(string key) => redisClient.SMembersAsync(key);

    public bool SIsMember(string key, string member) => redisClient.SIsMember(key, member);

    public Task<bool> SIsMemberAsync(string key, string member) => redisClient.SIsMemberAsync(key, member);

    public long SCard(string key) => redisClient.SCard(key);

    public Task<long> SCardAsync(string key) => redisClient.SCardAsync(key);

    public bool Expire(string key, TimeSpan expiry) => redisClient.Expire(key, expiry);

    public Task<bool> ExpireAsync(string key, TimeSpan expiry) => redisClient.ExpireAsync(key, expiry);

    public bool Persist(string key) => redisClient.Persist(key);

    public Task<bool> PersistAsync(string key) => redisClient.PersistAsync(key);

    public long Del(params string[] keys) => redisClient.Del(keys);

    public Task<long> DelAsync(params string[] keys) => redisClient.DelAsync(keys);

    public long HSet(string key, string field, string value) => redisClient.HSet(key, field, value);

    public Task<long> HSetAsync(string key, string field, string value) => redisClient.HSetAsync(key, field, value);

    public long HSet(string key, Dictionary<string, string> keyValues) => redisClient.HSet(key, keyValues);

    public Task<long> HSetAsync(string key, Dictionary<string, string> keyValues) => redisClient.HSetAsync(key, keyValues);

    public string? HGet(string key, string field) => redisClient.HGet(key, field);

    public Task<string?> HGetAsync(string key, string field) => redisClient.HGetAsync(key, field);

    public string?[]? HMGet(string key, params string[] fields)
    {
        var result = redisClient.HMGet(key, fields).ToList();
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
        var result = (await redisClient.HMGetAsync(key, fields)).ToList();
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

    public Dictionary<string, string> HGetAll(string key) => redisClient.HGetAll(key);

    public long HLen(string key) => redisClient.HLen(key);

    public Task<long> HLenAsync(string key) => redisClient.HLenAsync(key);

    public long HDel(string key, params string[] fields) => redisClient.HDel(key, fields);

    public Task<long> HDelAsync(string key, params string[] fields) => redisClient.HDelAsync(key, fields);

    public bool HExists(string key, string field) => redisClient.HExists(key, field);

    public Task<bool> HExistsAsync(string key, string field) => redisClient.HExistsAsync(key, field);

    public long RPush(string key, params string[] elements) => redisClient.RPush(key, elements.Select(object (x) => x).ToArray());

    public Task<long> RPushAsync(string key, params string[] elements) => redisClient.RPushAsync(key, elements.Select(object (x) => x).ToArray());

    public long LPush(string key, params string[] elements) => redisClient.LPush(key, elements.Select(object (x) => x).ToArray());

    public Task<long> LPushAsync(string key, params string[] elements) => redisClient.LPushAsync(key, elements.Select(object (x) => x).ToArray());

    public string? RPopLPush(string source, string destination) => redisClient.RPopLPush(source, destination);

    public Task<string?> RPopLPushAsync(string source, string destination) => redisClient.RPopLPushAsync(source, destination);

    public long LRem(string key, long count, string element) => redisClient.LRem(key, count, element);

    public Task<long> LRemAsync(string key, long count, string element) => redisClient.LRemAsync(key, count, element);

    public void LTrim(string key, long start, long stop) => redisClient.LTrim(key, start, stop);

    public Task LTrimAsync(string key, long start, long stop) => redisClient.LTrimAsync(key, start, stop);

    public string[] LRange(string key, long start, long stop) => redisClient.LRange(key, start, stop);

    public Task<string[]> LRangeAsync(string key, long start, long stop) => redisClient.LRangeAsync(key, start, stop);

    public long LLen(string key) => redisClient.LLen(key);

    public Task<long> LLenAsync(string key) => redisClient.LLenAsync(key);

    public string? LIndex(string key, long index) => redisClient.LIndex(key, index);

    public Task<string?> LIndexAsync(string key, long index) => redisClient.LIndexAsync(key, index);

    public string? RPop(string key) => redisClient.RPop(key);

    public Task<string?> RPopAsync(string key) => redisClient.RPopAsync(key);

    public string? LPop(string key) => redisClient.LPop(key);

    public Task<string?> LPopAsync(string key) => redisClient.LPopAsync(key);

    public long IncrBy(string key, long increment) => redisClient.IncrBy(key, increment);

    public Task<long> IncrByAsync(string key, long increment) => redisClient.IncrByAsync(key, increment);

    public bool LockExtend(string key, string value, TimeSpan expiry)
    {
        var luaScript = @"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('EXPIRE', KEYS[1], ARGV[2])
            else
                return 0
            end
        ";

        var result = redisClient.Eval(luaScript, [key], [
            value,
            expiry.TotalSeconds.ToString()
        ]);
        return result?.ToString() == "1";
    }

    public bool LockTake(string key, string value, TimeSpan expiry) => redisClient.SetNx(key, value, expiry);

    public bool LockRelease(string key, string value)
    {
        var luaScript = @"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            else
                return 0
            end
        ";
        var result = redisClient.Eval(luaScript, [key], [value]);
        return result?.ToString() == "1";
    }

    public long? Ttl(string key) => redisClient.Ttl(key);

    public async Task<long?> TtlAsync(string key) => (await redisClient.TtlAsync(key));

    public bool Exists(string key) => redisClient.Exists(key);

    public Task<bool> ExistsAsync(string key) => redisClient.ExistsAsync(key);

    public DateTime Time()
    {
        var result = redisClient.Eval("return redis.call('TIME')", null, null);
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