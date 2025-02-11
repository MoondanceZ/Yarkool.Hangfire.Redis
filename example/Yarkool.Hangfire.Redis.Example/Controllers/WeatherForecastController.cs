using Hangfire;
using Microsoft.AspNetCore.Mvc;

namespace Yarkool.Hangfire.Redis.Example.Controllers;

[ApiController]
[Route("[controller]")]
public class WeatherForecastController
(
    IBackgroundJobClient backgroundJobClient,
    ILogger<WeatherForecastController> logger
) : ControllerBase
{
    private static readonly string[] Summaries = new[]
    {
        "Freezing",
        "Bracing",
        "Chilly",
        "Cool",
        "Mild",
        "Warm",
        "Balmy",
        "Hot",
        "Sweltering",
        "Scorching"
    };

    private readonly IBackgroundJobClient _backgroundJobClient = backgroundJobClient;
    private readonly ILogger<WeatherForecastController> _logger = logger;

    [HttpGet(Name = "GetWeatherForecast")]
    public IEnumerable<WeatherForecast> Get()
    {
        return Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                Date = DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
                TemperatureC = Random.Shared.Next(-20, 55),
                Summary = Summaries[Random.Shared.Next(Summaries.Length)]
            })
            .ToArray();
    }

    [HttpPost(Name = "Push")]
    public string Push()
    {
        _backgroundJobClient.Enqueue(() => Console.WriteLine($"推送测试: {DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}"));
        return "success";
    }
}