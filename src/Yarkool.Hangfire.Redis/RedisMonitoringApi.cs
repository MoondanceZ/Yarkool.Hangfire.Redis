using System.Collections.Concurrent;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Yarkool.Hangfire.Redis
{
    public class RedisMonitoringApi : JobStorageMonitor
    {
        private readonly RedisStorage _storage;
        private readonly IRedisClient _redisClient;

        private static readonly string[] sourceArray = new[]
        {
            "StartedAt",
            "ServerName",
            "ServerId",
            "State"
        };

        public RedisMonitoringApi([NotNull] RedisStorage storage)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));

            _storage = storage;
            _redisClient = storage.RedisClient;
        }

        public override long ScheduledCount()
        {
            return _redisClient.ZCount(_storage.GetRedisKey("schedule"));
        }

        public override IDictionary<DateTime, long> DeletedByDatesCount()
        {
            return GetHourlyTimelineStats("deleted");
        }

        public override IDictionary<DateTime, long> HourlyDeletedJobs()
        {
            return GetHourlyTimelineStats("deleted");
        }

        public override long EnqueuedCount([NotNull] string queue)
        {
            if (queue == null)
                throw new ArgumentNullException(nameof(queue));

            return _redisClient.LLen(_storage.GetRedisKey($"queue:{queue}"));
        }

        public override long FetchedCount([NotNull] string queue)
        {
            if (queue == null)
                throw new ArgumentNullException(nameof(queue));

            return _redisClient.LLen(_storage.GetRedisKey($"queue:{queue}:dequeued"));
        }

        public override long ProcessingCount()
        {
            return _redisClient.ZCount(_storage.GetRedisKey("processing"));
        }

        public override long SucceededListCount()
        {
            return _redisClient.LLen(_storage.GetRedisKey("succeeded"));
        }

        public override long FailedCount()
        {
            return _redisClient.LLen(_storage.GetRedisKey("failed"));
        }

        public override long DeletedListCount()
        {
            return _redisClient.LLen(_storage.GetRedisKey("deleted"));
        }

        public override JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            var jobIds = _redisClient
                .ZRangeByRank(_storage.GetRedisKey("processing"), from, from + count - 1)
                .ToArray();

            return new JobList<ProcessingJobDto>(GetJobsWithProperties(
                    jobIds,
                    null,
                    sourceArray,
                    false,
                    (job, jobData, state, historyData, loadException) => new ProcessingJobDto
                    {
                        ServerId = state[2] ?? state[1],
                        Job = job,
                        StartedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        InProcessingState = ProcessingState.StateName.Equals(
                            state[3], StringComparison.OrdinalIgnoreCase)
                    })
                .Where(x => x.Value?.ServerId != null)
                .OrderBy(x => x.Value.StartedAt).ToList());
        }

        public override JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            var scheduledJobs = _redisClient
                .ZRangeByRankWithScores(_storage.GetRedisKey("schedule"), from, from + count - 1)
                .ToList();

            if (scheduledJobs.Count == 0)
            {
                return new JobList<ScheduledJobDto>(new List<KeyValuePair<string, ScheduledJobDto>>());
            }

            var jobs = new ConcurrentDictionary<string, List<string>>();
            var states = new ConcurrentDictionary<string, List<string>>();
            var jobIds = new Dictionary<string, string>();

            var tasks = new Task[scheduledJobs.Count * 2];
            int i = 0;
            foreach (var scheduledJob in scheduledJobs)
            {
                var jobInfo = scheduledJob.Element.Split(':');
                var jobId = scheduledJob.Element;
                jobIds.Add(scheduledJob.Element, jobId);
                tasks[i] = _redisClient.HMGetAsync(_storage.GetRedisKey($"job:{jobId}"),
                [
                    "Type",
                    "Method",
                    "ParameterTypes",
                    "Arguments"
                ]).ContinueWith(x => jobs.TryAdd(scheduledJob.Element, x.Result!.ToList()!));
                i++;
                tasks[i] = _redisClient.HMGetAsync(_storage.GetRedisKey($"job:{jobId}:state"),
                [
                    "State",
                    "ScheduledAt"
                ]).ContinueWith(x => states.TryAdd(scheduledJob.Element, x.Result!.ToList()!));
                i++;
            }

            Task.WaitAll(tasks);

            return new JobList<ScheduledJobDto>(scheduledJobs.Select(job => new KeyValuePair<string, ScheduledJobDto>(jobIds[job.Element], new ScheduledJobDto
            {
                EnqueueAt = JobHelper.FromTimestamp((long)job.Score),
                Job = TryToGetJob(jobs[job.Element][0], jobs[job.Element][1], jobs[job.Element][2], jobs[job.Element][3], out var loadException),
                ScheduledAt = states[job.Element].Count > 1 ? JobHelper.DeserializeNullableDateTime(states[job.Element][1]) : null,
                InScheduledState = ScheduledState.StateName.Equals(states[job.Element][0], StringComparison.OrdinalIgnoreCase)
            })).ToList());
        }

        public override IDictionary<DateTime, long> SucceededByDatesCount()
        {
            return GetTimelineStats("succeeded");
        }

        public override IDictionary<DateTime, long> FailedByDatesCount()
        {
            return GetTimelineStats("failed");
        }

        public override IList<ServerDto> Servers()
        {
            var serverNames = _redisClient
                .SMembers(_storage.GetRedisKey("servers"))
                .Select(x => (string)x)
                .ToList();

            if (serverNames.Count == 0)
            {
                return new List<ServerDto>();
            }

            var servers = new List<ServerDto>();

            foreach (var serverName in serverNames)
            {
                var queue = _redisClient.LRange(_storage.GetRedisKey($"server:{serverName}:queues"), 0, -1).ToList();

                var server = _redisClient.HMGet(_storage.GetRedisKey($"server:{serverName}"), [
                    "WorkerCount",
                    "StartedAt",
                    "Heartbeat"
                ])?.ToList() ?? [];
                if (server.Count == 0)
                    continue; // skip removed server

                servers.Add(new ServerDto
                {
                    Name = serverName,
                    WorkersCount = int.Parse(server[0]!),
                    Queues = queue,
                    StartedAt = JobHelper.DeserializeDateTime(server[1]),
                    Heartbeat = JobHelper.DeserializeNullableDateTime(server[2])
                });
            }

            return servers;
        }

        public override JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            var failedJobIds = _redisClient
                .ZRevRangeByRank(_storage.GetRedisKey("failed"), from, from + count - 1)
                .ToArray();

            return GetJobsWithProperties(
                failedJobIds,
                null,
                [
                    "FailedAt",
                    "ExceptionType",
                    "ExceptionMessage",
                    "ExceptionDetails",
                    "State",
                    "Reason"
                ],
                false,
                (job, jobData, state, historyData, loadException) => new FailedJobDto
                {
                    Job = job,
                    Reason = state[5],
                    FailedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                    ExceptionType = state[1],
                    ExceptionMessage = state[2],
                    ExceptionDetails = state[3],
                    InFailedState = FailedState.StateName.Equals(state[4], StringComparison.OrdinalIgnoreCase)
                });
        }

        public override JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            var succeededJobIds = _redisClient
                .LRange(_storage.GetRedisKey("succeeded"), from, from + count - 1)
                .ToArray();

            return GetJobsWithProperties(
                succeededJobIds,
                null,
                [
                    "SucceededAt",
                    "PerformanceDuration",
                    "Latency",
                    "State",
                    "Result"
                ],
                false,
                (job, jobData, state, historyData, loadException) => new SucceededJobDto
                {
                    Job = job,
                    Result = state[4],
                    SucceededAt = JobHelper.DeserializeNullableDateTime(state[0]),
                    TotalDuration = state[1] != null! && state[2] != null! ? (long?)long.Parse(state[1]) + (long?)long.Parse(state[2]) : null,
                    InSucceededState = SucceededState.StateName.Equals(state[3], StringComparison.OrdinalIgnoreCase)
                });
        }

        public override JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            var deletedJobIds = _redisClient
                .LRange(_storage.GetRedisKey("deleted"), from, from + count - 1)
                .ToArray();

            return GetJobsWithProperties(
                deletedJobIds,
                null,
                [
                    "DeletedAt",
                    "State"
                ],
                false,
                (job, jobData, state, historyData, loadException) => new DeletedJobDto
                {
                    Job = job,
                    DeletedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                    InDeletedState = DeletedState.StateName.Equals(state[1], StringComparison.OrdinalIgnoreCase)
                });
        }

        public override IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            var queues = _redisClient
                .SMembers(_storage.GetRedisKey("queues"))
                .Select(x => (string)x)
                .ToList();

            var result = new List<QueueWithTopEnqueuedJobsDto>(queues.Count);

            foreach (var queue in queues)
            {
                string[]? firstJobIds = null;
                long length = 0;
                long fetched = 0;

                var tasks = new Task[3];
                tasks[0] = _redisClient.LRangeAsync(_storage.GetRedisKey($"queue:{queue}"), -5, -1).ContinueWith(x => firstJobIds = x.Result);

                tasks[1] = _redisClient.LLenAsync(_storage.GetRedisKey($"queue:{queue}")).ContinueWith(x => length = x.Result);

                tasks[2] = _redisClient.LLenAsync(_storage.GetRedisKey($"queue:{queue}:dequeued")).ContinueWith(x => fetched = x.Result);

                Task.WaitAll(tasks);

                var jobs = GetJobsWithProperties(
                    firstJobIds!,
                    ["State"],
                    [
                        "EnqueuedAt",
                        "State"
                    ],
                    false,
                    (job, jobData, state, historyData, loadException) => new EnqueuedJobDto
                    {
                        Job = job,
                        State = jobData[0],
                        EnqueuedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        InEnqueuedState = jobData[0].Equals(state[1], StringComparison.OrdinalIgnoreCase)
                    });

                result.Add(new QueueWithTopEnqueuedJobsDto
                {
                    Name = queue,
                    FirstJobs = jobs,
                    Length = length,
                    Fetched = fetched
                });
            }

            return result;
        }

        public override JobList<EnqueuedJobDto> EnqueuedJobs([NotNull] string queue, int from, int count)
        {
            if (queue == null)
                throw new ArgumentNullException(nameof(queue));

            var jobIds = _redisClient
                .LRange(_storage.GetRedisKey($"queue:{queue}"), from, from + count - 1)
                .ToArray();

            return GetJobsWithProperties(
                jobIds,
                ["State"],
                [
                    "EnqueuedAt",
                    "State"
                ],
                false,
                (job, jobData, state, historyData, loadException) => new EnqueuedJobDto
                {
                    Job = job,
                    State = jobData[0],
                    EnqueuedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                    InEnqueuedState = jobData[0].Equals(state[1], StringComparison.OrdinalIgnoreCase)
                });
        }

        public override JobList<FetchedJobDto> FetchedJobs([NotNull] string queue, int from, int count)
        {
            if (queue == null)
                throw new ArgumentNullException(nameof(queue));

            var jobIds = _redisClient
                .LRange(_storage.GetRedisKey($"queue:{queue}:dequeued"), from, from + count - 1)
                .ToArray();

            return GetJobsWithProperties(
                jobIds,
                [
                    "State",
                    "Fetched"
                ],
                null,
                false,
                (job, jobData, state, historyData, loadException) => new FetchedJobDto
                {
                    Job = job,
                    State = jobData[0],
                    FetchedAt = JobHelper.DeserializeNullableDateTime(jobData[1])
                });
        }

        public override JobList<AwaitingJobDto> AwaitingJobs(int from, int count)
        {
            var awaitingJobIds = _redisClient
                .LRange(_storage.GetRedisKey("awaiting"), from, from + count - 1)
                .ToArray();

            return GetJobsWithProperties(
                awaitingJobIds,
                null,
                [
                    "Expiration",
                    "Options",
                    "State",
                    "NextState",
                    "ParentId"
                ],
                true,
                (job, jobData, stateData, historyData, loadException) => new AwaitingJobDto
                {
                    Job = job,
                    AwaitingAt = historyData.TryGetValue("CreatedAt", out var value) ? JobHelper.DeserializeNullableDateTime(value) : null,
                    InAwaitingState = AwaitingState.StateName.Equals(stateData[2], StringComparison.OrdinalIgnoreCase),
                    InvocationData = new InvocationData(jobData[0], jobData[1], jobData[2], jobData[3]), //should the third argument of GetJobsWithProperties pass something, add the number of elements to the index of the array of jobData in this line
                    StateData = new Dictionary<string, string>
                    {
                        { "Expiration", stateData[0] },
                        { "Options", stateData[1] },
                        { "State", stateData[2] },
                        { "NextState", stateData[3] },
                        { "ParentId", stateData[4] }
                    },
                    LoadException = new JobLoadException($"Error deserializing job", loadException),
                    ParentStateName = _storage.GetConnection().GetStateData(stateData[4]).Name
                });
        }

        public override IDictionary<DateTime, long> HourlySucceededJobs()
        {
            return GetHourlyTimelineStats("succeeded");
        }

        public override IDictionary<DateTime, long> HourlyFailedJobs()
        {
            return GetHourlyTimelineStats("failed");
        }

        public override JobDetailsDto JobDetails([NotNull] string jobId)
        {
            if (jobId == null)
                throw new ArgumentNullException(nameof(jobId));

            var job = _redisClient.HGetAll(_storage.GetRedisKey($"job:{jobId}"));

            if (job.Count == 0)
                return null!;

            var hiddenProperties = new[]
            {
                "Type",
                "Method",
                "ParameterTypes",
                "Arguments",
                "State",
                "CreatedAt",
                "Fetched"
            };

            var history = _redisClient
                .LRange(_storage.GetRedisKey($"job:{jobId}:history"), 0, -1)
                .Select(SerializationHelper.Deserialize<Dictionary<string, string>>)
                .ToList();

            // history is in wrong order, fix this
            history.Reverse();

            var stateHistory = new List<StateHistoryDto>(history.Count);
            foreach (var entry in history)
            {
                var stateData = new Dictionary<string, string>(entry, StringComparer.OrdinalIgnoreCase);
                var dto = new StateHistoryDto
                {
                    StateName = stateData["State"],
                    Reason = stateData.GetValueOrDefault("Reason"),
                    CreatedAt = JobHelper.DeserializeDateTime(stateData["CreatedAt"])
                };

                // Each history item contains all of the information,
                // but other code should not know this. We'll remove
                // unwanted keys.
                stateData.Remove("State");
                stateData.Remove("Reason");
                stateData.Remove("CreatedAt");

                dto.Data = stateData;
                stateHistory.Add(dto);
            }

            // For compatibility
            job.TryAdd("Type", null!);
            job.TryAdd("Method", null!);
            job.TryAdd("ParameterTypes", null!);
            job.TryAdd("Arguments", null!);

            return new JobDetailsDto
            {
                Job = TryToGetJob(job["Type"], job["Method"], job["ParameterTypes"], job["Arguments"], out var loadException),
                CreatedAt = job.TryGetValue("CreatedAt", out var value) ? JobHelper.DeserializeDateTime(value) : (DateTime?)null,
                Properties = job.Where(x => !hiddenProperties.Contains(x.Key)).ToDictionary(x => x.Key, x => x.Value),
                History = stateHistory
            };
        }

        private Dictionary<DateTime, long> GetHourlyTimelineStats([NotNull] string type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            var endDate = DateTime.UtcNow;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var keys = dates.Select(x => _storage.GetRedisKey($"stats:{type}:{x:yyyy-MM-dd-HH}")).ToArray();
            var valuesMap = _redisClient.GetValuesMap(keys);

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                long value;
                if (!long.TryParse(valuesMap[valuesMap.Keys.ElementAt(i)], out value))
                {
                    value = 0;
                }

                result.Add(dates[i], value);
            }

            return result;
        }

        private Dictionary<DateTime, long> GetTimelineStats([NotNull] string type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            var endDate = DateTime.UtcNow.Date;
            var startDate = endDate.AddDays(-7);
            var dates = new List<DateTime>();

            while (startDate <= endDate)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            var keys = dates.Select(x => _storage.GetRedisKey($"stats:{type}:{x:yyyy-MM-dd}")).ToArray();

            var valuesMap = _redisClient.GetValuesMap(keys);

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                long value;
                if (!long.TryParse(valuesMap[valuesMap.Keys.ElementAt(i)], out value))
                {
                    value = 0;
                }

                result.Add(dates[i], value);
            }

            return result;
        }

        private JobList<T> GetJobsWithProperties<T>(
            [NotNull] string[] jobIds,
            string[]? properties,
            string[]? stateProperties,
            bool loadHistory,
            [NotNull] Func<Job, IReadOnlyList<string>, IReadOnlyList<string>, Dictionary<string, string>, Exception, T> selector)
        {
            if (jobIds == null)
                throw new ArgumentNullException(nameof(jobIds));
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));
            if (jobIds.Length == 0)
                return new JobList<T>(new List<KeyValuePair<string, T>>());

            var jobs = new Dictionary<string, Task<string[]>>(jobIds.Length, StringComparer.OrdinalIgnoreCase);
            var states = new Dictionary<string, Task<string[]>>(jobIds.Length, StringComparer.OrdinalIgnoreCase);
            var histories = new Dictionary<string, Task<string>>(jobIds.Length, StringComparer.OrdinalIgnoreCase);

            properties ??= new string[0];

            var extendedProperties = properties
                .Concat([
                    "Type",
                    "Method",
                    "ParameterTypes",
                    "Arguments"
                ])
                .ToArray();

            var tasks = new List<Task>(jobIds.Length * 2);
            foreach (var jobId in jobIds.Distinct())
            {
                var jobTask = _redisClient.HMGetAsync(_storage.GetRedisKey($"job:{jobId}"), extendedProperties);
                tasks.Add(jobTask);
                jobs.Add(jobId, jobTask!);

                if (stateProperties != null)
                {
                    var taskStateJob = _redisClient.HMGetAsync(_storage.GetRedisKey($"job:{jobId}:state"), stateProperties);
                    tasks.Add(taskStateJob);
                    states.Add(jobId, taskStateJob!);
                }

                if (loadHistory)
                {
                    var taskHistoryJob = _redisClient.LIndexAsync(_storage.GetRedisKey($"job:{jobId}:history"), -1);
                    tasks.Add(taskHistoryJob);
                    histories.Add(jobId, taskHistoryJob!);
                }
            }

            Task.WaitAll(tasks.ToArray());

            var jobList = new JobList<T>(jobIds
                .Select(jobId => new
                {
                    JobId = jobId,
                    JobData = jobs[jobId].Result.ToArray(),
                    Job = TryToGetJob(
                        jobs[jobId].Result[properties.Length],
                        jobs[jobId].Result[properties.Length + 1],
                        jobs[jobId].Result[properties.Length + 2],
                        jobs[jobId].Result[properties.Length + 3],
                        out Exception? loadException),
                    LastHistoryEntry = !histories.TryGetValue(jobId, out var history) ? null : SerializationHelper.Deserialize<Dictionary<string, string>>(history.ToString()),
                    LoadException = loadException,
                    State = stateProperties != null ? states[jobId].Result.ToArray() : null
                })
                .Select(x => new KeyValuePair<string, T>(x.JobId, x.JobData.Any(y => y != null!) ?
                    selector(x.Job!, x.JobData, x.State!, x.LastHistoryEntry!, x.LoadException!)! :
                    default!)));
            return jobList;
        }

        public override StatisticsDto GetStatistics()
        {
            var stats = new StatisticsDto();

            var queues = _redisClient.SMembers(_storage.GetRedisKey("queues"));

            using var pipeline = _redisClient.BeginPipeline();
            pipeline.SCard(_storage.GetRedisKey("servers"));
            pipeline.SCard(_storage.GetRedisKey("queues"));
            pipeline.ZCard(_storage.GetRedisKey("schedule"));
            pipeline.ZCard(_storage.GetRedisKey("processing"));
            pipeline.Get(_storage.GetRedisKey("stats:succeeded"));
            pipeline.ZCard(_storage.GetRedisKey("failed"));
            pipeline.Get(_storage.GetRedisKey("stats:deleted"));
            pipeline.ZCard(_storage.GetRedisKey("recurring-jobs"));

            foreach (var queue in queues)
            {
                pipeline.LLen(_storage.GetRedisKey($"queue:{queue}"));
            }

            var result = pipeline.Execute();
            if (result is { Length: >= 8 })
            {
                stats.Servers = result[0] == null ? 0 : Convert.ToInt64(result[0]);
                stats.Queues = result[1] == null ? 0 : Convert.ToInt64(result[1]);
                stats.Scheduled = result[2] == null ? 0 : Convert.ToInt64(result[2]);
                stats.Processing = result[3] == null ? 0 : Convert.ToInt64(result[3]);
                stats.Succeeded = result[4] == null ? 0 : Convert.ToInt64(result[4]);
                stats.Failed = result[5] == null ? 0 : Convert.ToInt64(result[5]);
                stats.Deleted = result[6] == null ? 0 : Convert.ToInt64(result[6]);
                stats.Recurring = result[7] == null ? 0 : Convert.ToInt64(result[7]);
                for (var i = 8; i < result.Length; i++)
                {
                    stats.Enqueued += Convert.ToInt64(result[i]);
                }
            }

            return stats;
        }

        private static Job? TryToGetJob(string type, string method, string parameterTypes, string arguments, out Exception? loadException)
        {
            try
            {
                loadException = null;
                return new InvocationData(type, method, parameterTypes, arguments).DeserializeJob();
            }
            catch (Exception e)
            {
                loadException = e;
                return null;
            }
        }

        public override long AwaitingCount()
        {
            return _redisClient.ZCard(_storage.GetRedisKey("awaiting"));
        }
    }
}