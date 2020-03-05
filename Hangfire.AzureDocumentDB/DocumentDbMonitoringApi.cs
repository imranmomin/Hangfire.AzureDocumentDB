﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Hangfire.Azure.Documents;
using Hangfire.Azure.Helper;
using Hangfire.Azure.Queue;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.SystemFunctions;

namespace Hangfire.Azure
{
    internal sealed class DocumentDbMonitoringApi : IMonitoringApi
    {
        private readonly DocumentDbStorage storage;
        private readonly object cacheLock = new object();
        private static readonly TimeSpan cacheTimeout = TimeSpan.FromSeconds(2);
        private static DateTime cacheUpdated;
        private static StatisticsDto cacheStatisticsDto;

        public DocumentDbMonitoringApi(DocumentDbStorage storage) => this.storage = storage;

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            List<QueueWithTopEnqueuedJobsDto> queueJobs = new List<QueueWithTopEnqueuedJobsDto>();

            var tuples = storage.QueueProviders
                .Select(x => x.GetJobQueueMonitoringApi())
                .SelectMany(x => x.GetQueues(), (monitoring, queue) => new { Monitoring = monitoring, Queue = queue })
                .OrderBy(x => x.Queue)
                .ToArray();

            foreach (var tuple in tuples)
            {
                (int? EnqueuedCount, int? FetchedCount) counters = tuple.Monitoring.GetEnqueuedAndFetchedCount(tuple.Queue);
                JobList<EnqueuedJobDto> jobs = EnqueuedJobs(tuple.Queue, 0, 5);

                queueJobs.Add(new QueueWithTopEnqueuedJobsDto
                {
                    Length = counters.EnqueuedCount ?? 0,
                    Fetched = counters.FetchedCount ?? 0,
                    Name = tuple.Queue,
                    FirstJobs = jobs
                });
            }

            return queueJobs;
        }

        public IList<ServerDto> Servers()
        {
            return storage.Client.CreateDocumentQuery<Documents.Server>(storage.CollectionUri, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Server) })
                .Where(s => s.DocumentType == DocumentTypes.Server)
                .OrderByDescending(s => s.CreatedOn)
                .ToQueryResult()
                .Select(server => new ServerDto
                {
                    Name = server.ServerId,
                    Heartbeat = server.LastHeartbeat,
                    Queues = server.Queues,
                    StartedAt = server.CreatedOn,
                    WorkersCount = server.Workers
                }).ToList();
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

            Uri uri = UriFactory.CreateDocumentUri(storage.Options.DatabaseName, storage.Options.CollectionName, jobId);
            Task<DocumentResponse<Documents.Job>> task = storage.Client.ReadDocumentWithRetriesAsync<Documents.Job>(uri, new RequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Job) });
            task.Wait();

            if (task.Result.Document != null)
            {
                Documents.Job job = task.Result;
                InvocationData invocationData = job.InvocationData;
                invocationData.Arguments = job.Arguments;

                List<StateHistoryDto> states = storage.Client.CreateDocumentQuery<State>(storage.CollectionUri, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.State) })
                    .Where(s => s.DocumentType == DocumentTypes.State && s.JobId == jobId)
                    .OrderByDescending(s => s.CreatedOn)
                    .ToQueryResult()
                    .Select(s => new StateHistoryDto
                    {
                        Data = s.Data,
                        CreatedAt = s.CreatedOn,
                        Reason = s.Reason,
                        StateName = s.Name
                    }).ToList();

                return new JobDetailsDto
                {
                    Job = invocationData.DeserializeJob(),
                    CreatedAt = job.CreatedOn,
                    ExpireAt = job.ExpireOn,
                    Properties = job.Parameters.ToDictionary(p => p.Name, p => p.Value),
                    History = states
                };
            }

            return null;
        }

        public StatisticsDto GetStatistics()
        {
            lock (cacheLock)
            {
                if (cacheStatisticsDto == null || cacheUpdated.Add(cacheTimeout) < DateTime.UtcNow)
                {
                    Dictionary<string, long> results = new Dictionary<string, long>();

                    // get counts of jobs on state
                    string[] keys = { EnqueuedState.StateName, FailedState.StateName, ProcessingState.StateName, ScheduledState.StateName, SucceededState.StateName, AwaitingState.StateName };
                    List<IGrouping<string, string>> states = storage.Client.CreateDocumentQuery<Documents.Job>(storage.CollectionUri, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Job) })
                         .Where(x => x.DocumentType == DocumentTypes.Job && x.StateName.IsDefined() && keys.Contains(x.StateName))
                         .Select(x => x.StateName)
                         .ToQueryResult()
                         .GroupBy(x => x)
                         .ToList();

                    foreach (IGrouping<string, string> state in states)
                    {
                        results.Add(state.Key.ToLower(), state.LongCount());
                    }

                    // get counts of servers
                    SqlQuerySpec sql = new SqlQuerySpec
                    {
                        QueryText = "SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.type = @type",
                        Parameters = new SqlParameterCollection
                        {
                            new SqlParameter("@type", (int)DocumentTypes.Server)
                        }
                    };

                    long servers = storage.Client.CreateDocumentQuery<long>(storage.CollectionUri, sql, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Server) })
                        .ToQueryResult()
                        .FirstOrDefault();

                    results.Add("servers", servers);

                    // get sum of stats:succeeded / stats:deleted counters
                    keys = new[] { "stats:succeeded", "stats:deleted" };
                    List<IGrouping<string, Counter>> counters = storage.Client.CreateDocumentQuery<Counter>(storage.CollectionUri, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Counter) })
                         .Where(x => x.DocumentType == DocumentTypes.Counter && keys.Contains(x.Key))
                         .ToQueryResult()
                         .GroupBy(x => x.Key)
                         .ToList();

                    foreach (IGrouping<string, Counter> counter in counters)
                    {
                        long total = counter.Sum(x => x.Value);
                        results.Add(counter.Key.ToLower(), total);
                    }

                    sql = new SqlQuerySpec
                    {
                        QueryText = "SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.type = @type AND doc.key = @key",
                        Parameters = new SqlParameterCollection
                        {
                            new SqlParameter("@key", "recurring-jobs"),
                            new SqlParameter("@type", (int)DocumentTypes.Set)
                        }
                    };

                    long jobs = storage.Client.CreateDocumentQuery<long>(storage.CollectionUri, sql, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Set) })
                        .ToQueryResult()
                        .FirstOrDefault();

                    results.Add("recurring-jobs", jobs);

                    long getValueOrDefault(string key) => results.TryGetValue(key, out long value) ? value : default;

                    // ReSharper disable once UseObjectOrCollectionInitializer
                    cacheStatisticsDto = new StatisticsDto
                    {
                        Enqueued = getValueOrDefault("enqueued"),
                        Failed = getValueOrDefault("failed"),
                        Processing = getValueOrDefault("processing"),
                        Scheduled = getValueOrDefault("scheduled"),
                        Succeeded = getValueOrDefault("stats:succeeded"),
                        Deleted = getValueOrDefault("stats:deleted"),
                        Recurring = getValueOrDefault("recurring-jobs"),
                        Servers = getValueOrDefault("servers"),
                    };

                    cacheStatisticsDto.Queues = storage.QueueProviders
                        .SelectMany(x => x.GetJobQueueMonitoringApi().GetQueues())
                        .Count();

                    cacheUpdated = DateTime.UtcNow;
                }

                return cacheStatisticsDto;
            }
        }

        #region Job List

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
        {
            string queryText = $"SELECT * FROM doc WHERE doc.type = @type AND doc.name = @name AND NOT IS_DEFINED(doc.fetched_at) ORDER BY doc.created_on OFFSET {from} LIMIT {perPage}";
            return GetJobsOnQueue(queryText, queue, (state, job, fetchedAt) => new EnqueuedJobDto
            {
                Job = job,
                State = state.Name,
                InEnqueuedState = EnqueuedState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
                EnqueuedAt = EnqueuedState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase)
                   ? JobHelper.DeserializeNullableDateTime(state.Data["EnqueuedAt"])
                   : null
            });
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
        {
            string queryText = $"SELECT * FROM doc WHERE doc.type = @type AND doc.name = @name AND IS_DEFINED(doc.fetched_at) ORDER BY doc.created_on OFFSET {from} LIMIT {perPage}";
            return GetJobsOnQueue(queryText, queue, (state, job, fetchedAt) => new FetchedJobDto
            {
                Job = job,
                State = state.Name,
                FetchedAt = fetchedAt
            });
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            return GetJobsOnState(ProcessingState.StateName, from, count, (state, job) => new ProcessingJobDto
            {
                Job = job,
                InProcessingState = ProcessingState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
                ServerId = state.Data.ContainsKey("ServerId") ? state.Data["ServerId"] : state.Data["ServerName"],
                StartedAt = JobHelper.DeserializeDateTime(state.Data["StartedAt"])
            });
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return GetJobsOnState(ScheduledState.StateName, from, count, (state, job) => new ScheduledJobDto
            {
                Job = job,
                InScheduledState = ScheduledState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
                EnqueueAt = JobHelper.DeserializeDateTime(state.Data["EnqueueAt"]),
                ScheduledAt = JobHelper.DeserializeDateTime(state.Data["ScheduledAt"])
            });
        }

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return GetJobsOnState(SucceededState.StateName, from, count, (state, job) => new SucceededJobDto
            {
                Job = job,
                InSucceededState = SucceededState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
                Result = state.Data.ContainsKey("Result") ? state.Data["Result"] : null,
                TotalDuration = state.Data.ContainsKey("PerformanceDuration") && state.Data.ContainsKey("Latency")
                                ? (long?)long.Parse(state.Data["PerformanceDuration"]) + long.Parse(state.Data["Latency"])
                                : null,
                SucceededAt = JobHelper.DeserializeNullableDateTime(state.Data["SucceededAt"])
            });
        }

        public JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return GetJobsOnState(FailedState.StateName, from, count, (state, job) => new FailedJobDto
            {
                Job = job,
                InFailedState = FailedState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
                Reason = state.Reason,
                FailedAt = JobHelper.DeserializeNullableDateTime(state.Data["FailedAt"]),
                ExceptionDetails = state.Data["ExceptionDetails"],
                ExceptionMessage = state.Data["ExceptionMessage"],
                ExceptionType = state.Data["ExceptionType"]
            });
        }

        public JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            return GetJobsOnState(DeletedState.StateName, from, count, (state, job) => new DeletedJobDto
            {
                Job = job,
                InDeletedState = DeletedState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
                DeletedAt = JobHelper.DeserializeNullableDateTime(state.Data["DeletedAt"])
            });
        }

        private JobList<T> GetJobsOnState<T>(string stateName, int from, int count, Func<State, Common.Job, T> selector)
        {
            List<KeyValuePair<string, T>> jobs = new List<KeyValuePair<string, T>>();

            List<Documents.Job> filterJobs = storage.Client.CreateDocumentQuery<Documents.Job>(storage.CollectionUri, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Job) })
                .Where(j => j.DocumentType == DocumentTypes.Job && j.StateName == stateName)
                .OrderByDescending(j => j.CreatedOn)
                .Skip(from).Take(count)
                .ToQueryResult()
                .ToList();

            filterJobs.ForEach(job =>
            {
                Uri uri = UriFactory.CreateDocumentUri(storage.Options.DatabaseName, storage.Options.CollectionName, job.StateId);
                Task<DocumentResponse<State>> task = storage.Client.ReadDocumentWithRetriesAsync<State>(uri, new RequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.State) });
                task.Wait();

                if (task.Result.Document != null)
                {
                    State state = task.Result;
                    InvocationData invocationData = job.InvocationData;
                    invocationData.Arguments = job.Arguments;

                    T data = selector(state, invocationData.DeserializeJob());
                    jobs.Add(new KeyValuePair<string, T>(job.Id, data));
                }
            });

            return new JobList<T>(jobs);
        }

        private JobList<T> GetJobsOnQueue<T>(string queryText, string queue, Func<State, Common.Job, DateTime?, T> selector)
        {
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));
            List<KeyValuePair<string, T>> jobs = new List<KeyValuePair<string, T>>();

            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = queryText,
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@type", (int)DocumentTypes.Queue),
                    new SqlParameter("@name", queue)
                }
            };

            List<Documents.Queue> queues = storage.Client.CreateDocumentQuery<Documents.Queue>(storage.CollectionUri, sql, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Queue) })
                .ToQueryResult()
                .ToList();

            queues.ForEach(queueItem =>
            {
                Uri uri = UriFactory.CreateDocumentUri(storage.Options.DatabaseName, storage.Options.CollectionName, queueItem.JobId);
                Task<DocumentResponse<Documents.Job>> task = storage.Client.ReadDocumentWithRetriesAsync<Documents.Job>(uri, new RequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Job) });
                task.Wait();

                if (task.Result != null)
                {
                    Documents.Job job = task.Result;
                    InvocationData invocationData = job.InvocationData;
                    invocationData.Arguments = job.Arguments;

                    uri = UriFactory.CreateDocumentUri(storage.Options.DatabaseName, storage.Options.CollectionName, job.StateId);
                    Task<DocumentResponse<State>> stateTask = storage.Client.ReadDocumentWithRetriesAsync<State>(uri, new RequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.State) });

                    T data = selector(stateTask.Result, invocationData.DeserializeJob(), queueItem.FetchedAt);
                    jobs.Add(new KeyValuePair<string, T>(job.Id, data));
                }
            });

            return new JobList<T>(jobs);
        }

        #endregion

        #region Counts

        public long EnqueuedCount(string queue)
        {
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));

            IPersistentJobQueueProvider provider = storage.QueueProviders.GetProvider(queue);
            IPersistentJobQueueMonitoringApi monitoringApi = provider.GetJobQueueMonitoringApi();
            (int? EnqueuedCount, int? FetchedCount) counters = monitoringApi.GetEnqueuedAndFetchedCount(queue);
            return counters.EnqueuedCount ?? 0;
        }

        public long FetchedCount(string queue)
        {
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));

            IPersistentJobQueueProvider provider = storage.QueueProviders.GetProvider(queue);
            IPersistentJobQueueMonitoringApi monitoringApi = provider.GetJobQueueMonitoringApi();
            (int? EnqueuedCount, int? FetchedCount) counters = monitoringApi.GetEnqueuedAndFetchedCount(queue);
            return counters.FetchedCount ?? 0;
        }

        public long ScheduledCount() => GetNumberOfJobsByStateName(ScheduledState.StateName);

        public long FailedCount() => GetNumberOfJobsByStateName(FailedState.StateName);

        public long ProcessingCount() => GetNumberOfJobsByStateName(ProcessingState.StateName);

        public long SucceededListCount() => GetNumberOfJobsByStateName(SucceededState.StateName);

        public long DeletedListCount() => GetNumberOfJobsByStateName(DeletedState.StateName);

        private long GetNumberOfJobsByStateName(string state)
        {
            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.type = @type AND IS_DEFINED(doc.state_name) AND doc.state_name = @state",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@state", state),
                    new SqlParameter("@type", (int)DocumentTypes.Job)
                }
            };

            return storage.Client.CreateDocumentQuery<long>(storage.CollectionUri, sql, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Job) })
                  .ToQueryResult()
                  .FirstOrDefault();
        }

        public IDictionary<DateTime, long> SucceededByDatesCount() => GetDatesTimelineStats("succeeded");

        public IDictionary<DateTime, long> FailedByDatesCount() => GetDatesTimelineStats("failed");

        public IDictionary<DateTime, long> HourlySucceededJobs() => GetHourlyTimelineStats("succeeded");

        public IDictionary<DateTime, long> HourlyFailedJobs() => GetHourlyTimelineStats("failed");

        private Dictionary<DateTime, long> GetHourlyTimelineStats(string type)
        {
            DateTime endDate = DateTime.UtcNow;
            List<DateTime> dates = new List<DateTime>();
            for (int i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            Dictionary<string, DateTime> keys = dates.ToDictionary(x => $"stats:{type}:{x:yyyy-MM-dd-HH}", x => x);
            return GetTimelineStats(keys);
        }

        private Dictionary<DateTime, long> GetDatesTimelineStats(string type)
        {
            DateTime endDate = DateTime.UtcNow.Date;
            List<DateTime> dates = new List<DateTime>();
            for (int i = 0; i < 7; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            Dictionary<string, DateTime> keys = dates.ToDictionary(x => $"stats:{type}:{x:yyyy-MM-dd}", x => x);
            return GetTimelineStats(keys);
        }

        private Dictionary<DateTime, long> GetTimelineStats(Dictionary<string, DateTime> keys)
        {
            Dictionary<DateTime, long> result = keys.ToDictionary(k => k.Value, v => default(long));
            string[] filter = keys.Keys.ToArray();

            Dictionary<string, int> data = storage.Client.CreateDocumentQuery<Counter>(storage.CollectionUri, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Counter) })
                .Where(c => c.DocumentType == DocumentTypes.Counter)
                .Where(c => filter.Contains(c.Key))
                .Select(c => new { c.Key, c.Value })
                .ToQueryResult()
                .GroupBy(c => c.Key)
                .ToDictionary(k => k.Key, k => k.Sum(c => c.Value));

            foreach (string key in keys.Keys)
            {
                DateTime date = keys.Where(k => k.Key == key).Select(k => k.Value).First();
                result[date] = data.TryGetValue(key, out int value) ? value : 0;
            }

            return result;
        }

        #endregion
    }
}
