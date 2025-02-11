namespace Yarkool.Hangfire.Redis;

public partial interface IRedisCommand
{
    long Publish(string channel, string message);
    Task<long> PublishAsync(string channel, string message);
    IDisposable? Subscribe(string key, Action<string, object> handler);
    void UnSubscribe(string key);
    string? Get(string key);
    Task<string?> GetAsync(string key);
    string?[] MGet(params string[] keys);
    bool Set(string key, string value);
    Task<bool> SetAsync(string key, string value);
    bool Set(string key, string value, TimeSpan expiry);
    Task<bool> SetAsync(string key, string value, TimeSpan expiry);
    bool SetNx(string key, string value);
    Task<bool> SetNxAsync(string key, string value);
    bool SetNx(string key, string value, TimeSpan expiry);
    Task<bool> SetNxAsync(string key, string value, TimeSpan expiry);
    long ZAdd(string key, string member, decimal score);
    Task<long> ZAddAsync(string key, string member, decimal score);
    long ZAdd(string key, Dictionary<string, decimal> data);
    Task<long> ZAddAsync(string key, Dictionary<string, decimal> data);
    long ZRem(string key, params string[] members);
    Task<long> ZRemAsync(string key, params string[] members);
    long ZCard(string key);
    Task<long> ZCardAsync(string key);
    long ZCount(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue);
    Task<long> ZCountAsync(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue);
    ScanResult<ZMember> ZScan(string key, long cursor, string? pattern = null, long count = 0);
    string[] ZRangeByScore(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue, int skip = 0, int take = 0);
    Task<string[]> ZRangeByScoreAsync(string key, decimal min = decimal.MinValue, decimal max = decimal.MaxValue, int skip = 0, int take = 0);
    string[] ZRangeByRank(string key, long start, long stop);
    Task<string[]> ZRangeByRankAsync(string key, long start, long stop);
    string[] ZRevRangeByRank(string key, long start, long stop);
    Task<string[]> ZRevRangeByRankAsync(string key, long start, long stop);
    (string Element, decimal Score)[] ZRangeByRankWithScores(string key, long start, long stop);
    Task<(string Element, decimal Score)[]> ZRangeByRankWithScoresAsync(string key, long start, long stop);
    (string Element, decimal Score)[] ZRangeByScoreWithScores(string key, decimal min, decimal max, long offset = 0, long count = 0);
    Task<(string Element, decimal Score)[]> ZRangeByScoreWithScoresAsync(string key, decimal min, decimal max, long offset = 0, long count = 0);
    long? ZRank(string key, string member);
    Task<long?> ZRankAsync(string key, string member);
    decimal? ZScore(string key, string member);
    Task<decimal?> ZScoreAsync(string key, string member);
    long SAdd(string key, params string[] members);
    Task<long> SAddAsync(string key, params string[] members);
    long SRem(string key, string member);
    Task<long> SRemAsync(string key, string member);
    string[] SMembers(string key);
    Task<string[]> SMembersAsync(string key);
    bool SIsMember(string key, string member);
    Task<bool> SIsMemberAsync(string key, string member);
    long SCard(string key);
    Task<long> SCardAsync(string key);
    bool Expire(string key, TimeSpan expiry);
    Task<bool> ExpireAsync(string key, TimeSpan expiry);
    bool Persist(string key);
    Task<bool> PersistAsync(string key);
    long Del(params string[] keys);
    Task<long> DelAsync(params string[] keys);
    long HSet(string key, string field, string value);
    Task<long> HSetAsync(string key, string field, string value);
    long HSet(string key, Dictionary<string, string> keyValues);
    Task<long> HSetAsync(string key, Dictionary<string, string> keyValues);
    string? HGet(string key, string field);
    Task<string?> HGetAsync(string key, string field);
    string?[]? HMGet(string key, params string[] fields);
    Task<string?[]?> HMGetAsync(string key, params string[] fields);
    Dictionary<string, string> HGetAll(string key);
    long HLen(string key);
    Task<long> HLenAsync(string key);
    long HDel(string key, params string[] fields);
    Task<long> HDelAsync(string key, params string[] fields);
    bool HExists(string key, string field);
    Task<bool> HExistsAsync(string key, string field);
    long RPush(string key, params string[] elements);
    Task<long> RPushAsync(string key, params string[] elements);
    long LPush(string key, params string[] elements);
    Task<long> LPushAsync(string key, params string[] elements);
    string? RPopLPush(string source, string destination);
    Task<string?> RPopLPushAsync(string source, string destination);
    long LRem(string key, long count, string element);
    Task<long> LRemAsync(string key, long count, string element);
    void LTrim(string key, long start, long stop);
    Task LTrimAsync(string key, long start, long stop);
    string[] LRange(string key, long start, long stop);
    Task<string[]> LRangeAsync(string key, long start, long stop);
    long LLen(string key);
    Task<long> LLenAsync(string key);
    string? LIndex(string key, long index);
    Task<string?> LIndexAsync(string key, long index);
    string? RPop(string key);
    Task<string?> RPopAsync(string key);
    string? LPop(string key);
    Task<string?> LPopAsync(string key);
    long IncrBy(string key, long increment);
    Task<long> IncrByAsync(string key, long increment);
    bool LockExtend(string key, string value, TimeSpan expiry);
    bool LockTake(string key, string value, TimeSpan expiry);
    bool LockRelease(string key, string value);
    long? Ttl(string key);
    Task<long?> TtlAsync(string key);
    bool Exists(string key);
    Task<bool> ExistsAsync(string key);
    DateTime Time();

    #region result object
    public class ScanResult<T>
    {
        public readonly long Cursor;
        public readonly T[] Items;
        public readonly long Length;

        public ScanResult(long cursor, T[] items)
        {
            Cursor = cursor;
            Items = items;
            Length = items.LongLength;
        }
    }

    public class ZMember
    {
        public readonly string Member;
        public readonly decimal Score;

        public ZMember(string member, decimal score)
        {
            Member = member;
            Score = score;
        }
    }
    #endregion
}

public interface IRedisCommand<out TCommandCall> : IRedisCommand where TCommandCall : IRedisCommandCall
{
}