using System.Globalization;
using FreeRedis;
using Hangfire;
using Microsoft.AspNetCore.Localization;
using Yarkool.Hangfire.Redis.FreeRedis;
using Yarkool.Hangfire.Redis.SharpRedis;

namespace Yarkool.Hangfire.Redis.Example;

public class Program
{
    private const string redisConn = "127.0.0.1,port=6379";
    private const string redisLibrary = "SharpRedis"; //FreeRedis, SharpRedis
    // private static IRedisClient redisClient = default!;

    public static void Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        // Add services to the container.

        builder.Services.AddControllers();
        // Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
        builder.Services.AddEndpointsApiExplorer();
        builder.Services.AddSwaggerGen();

        IRedisClient redisClient = redisLibrary == "FreeRedis" ?
            new FreeRedisClient(new RedisClient(redisConn)) :
            new SharpRedisClient(global::SharpRedis.Redis.UseStandalone($"host={redisConn}"));

        var storage = new RedisStorage(redisClient, new RedisStorageOptions { Prefix = "sss:" });

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

        var app = builder.Build();

        // Configure the HTTP request pipeline.
        if (app.Environment.IsDevelopment())
        {
            app.UseSwagger();
            app.UseSwaggerUI();
        }

        app.UseHttpsRedirection();

        app.UseAuthorization();

        app.MapControllers();

        // 默认区域性
        var supportedCultures = new[] { new CultureInfo("zh-CN") };
        app.UseRequestLocalization(new RequestLocalizationOptions
        {
            DefaultRequestCulture = new RequestCulture("zh-CN"),
            // Formatting numbers, dates, etc.
            SupportedCultures = supportedCultures,
            // UI strings that we have localized.
            SupportedUICultures = supportedCultures,
            RequestCultureProviders = new List<IRequestCultureProvider>
            {
                new QueryStringRequestCultureProvider(),
                new CookieRequestCultureProvider(),
                new AcceptLanguageHeaderRequestCultureProvider()
            }
        });
        app.UseHangfireDashboard(options: new DashboardOptions
        {
            IgnoreAntiforgeryToken = true,
            DisplayStorageConnectionString = false, // 是否显示数据库连接信息
            IsReadOnlyFunc = context => false
        });
        //app.UseHangfireServer(new BackgroundJobServerOptions
        //{
        //    Queues = new []{"dev","test","pred","prod","default"}
        //});
        RecurringJob.AddOrUpdate("test_console", () => Console.WriteLine($"定时任务输出: {DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}"), "*/1 * * * * ? ", new RecurringJobOptions { TimeZone = TimeZoneInfo.Local });
        for (var i = 0; i <= 50; i++)
            BackgroundJob.Schedule(() => Console.WriteLine($"延迟任务输出: {DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}"), TimeSpan.FromMinutes(1 + i));

        app.Run();
    }
}