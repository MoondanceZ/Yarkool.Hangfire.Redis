using Hangfire;
using Microsoft.AspNetCore.Mvc;

namespace Yarkool.Hangfire.Redis.Example.Controllers;

[ApiController]
[Route("[controller]")]
public class WeatherForecastController
(
    IBackgroundJobClient backgroundJobClient
) : ControllerBase
{
    private static readonly string[] Summaries =
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
        backgroundJobClient.Enqueue(() => Console.WriteLine($"推送测试: {DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}"));
        return "success";
    }
}