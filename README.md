# Yarkool.Hangfire.Redis

Hangfire 的 Redis 实现, 支持 `FreeRedis`, `SharpRedis`, `ServiceStack.Redis`

## 教程

创建 `Storage`, 如果使用 `FreeRedis` 则引入 `Yarkool.Hangfire.Redis.FreeRedis`, 使用 `SharpRedis` 则引入
`Yarkool.Hangfire.Redis.SharpRedis`, 使用 `ServiceStack.Redis` 则引入
`Yarkool.Hangfire.Redis.ServiceStack`

```csharp
// Redis 链接字符串
var  redisConn = "127.0.0.1,port=6379"

// FreeRedis
var redisClient = new FreeRedisClient(new RedisClient(redisConn)); 

// SharpRedis
var redisClient = new SharpRedisClient(global::SharpRedis.Redis.UseStandalone($"host={redisConn}"));

// ServiceStack.Redis, 该库收费, 使用前请提前注册密钥!!!
var redisClient = new ServiceStackClient(new RedisManagerPool(redisConn.Replace(",port", ":")));

// 创建 Storage
var storage = new RedisStorage(redisClient, new RedisStorageOptions { Prefix = "hangfire:" });
builder.Services.AddHangfire(o => o.UseStorage(storage));
builder.Services.AddHangfireServer((sp) =>
{
    sp.Queues =
    [
        "dev",
        "test",
        "pred",
        "prod",
        "default"
    ];
});

// 使用方式

// 添加任务
RecurringJob.AddOrUpdate("test_console", () => Console.WriteLine($"定时任务输出: {DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}"), "*/1 * * * * ? ", new RecurringJobOptions { TimeZone = TimeZoneInfo.Local });

// 或者使用注入方式
[ApiController]
[Route("[controller]")]
public class MessageController
(
    IBackgroundJobClient backgroundJobClient
) : ControllerBase
{    
    [HttpPost(Name = "Push")]
    public string Push()
    {
        _backgroundJobClient.Enqueue(() => Console.WriteLine($"推送测试: {DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}"));
        return "success";
    }
}
```