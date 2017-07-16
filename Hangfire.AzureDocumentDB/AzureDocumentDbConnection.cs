using System;
using System.Net;
using System.Linq;
using System.Threading;
using System.Globalization;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using Hangfire.AzureDocumentDB.Queue;
using Hangfire.AzureDocumentDB.Helper;
using Hangfire.AzureDocumentDB.Entities;

namespace Hangfire.AzureDocumentDB
{
    internal sealed class AzureDocumentDbConnection : JobStorageConnection
    {
        public AzureDocumentDbStorage Storage { get; }
        public PersistentJobQueueProviderCollection QueueProviders { get; }

        private readonly FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };

        public AzureDocumentDbConnection(AzureDocumentDbStorage storage)
        {
            Storage = storage;
            QueueProviders = storage.QueueProviders;
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout) => new AzureDocumentDbDistributedLock(resource, timeout, Storage);
        public override IWriteOnlyTransaction CreateWriteTransaction() => new AzureDocumentDbWriteOnlyTransaction(this);

        #region Job

        public override string CreateExpiredJob(Common.Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException(nameof(job));
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));

            InvocationData invocationData = InvocationData.Serialize(job);
            Entities.Job entityJob = new Entities.Job
            {
                InvocationData = invocationData,
                Arguments = invocationData.Arguments,
                CreatedOn = createdAt,
                ExpireOn = createdAt.Add(expireIn),

                Parameters = parameters.Select(p => new Parameter
                {
                    Name = p.Key,
                    Value = p.Value
                }).ToArray()
            };

            Task<ResourceResponse<Document>> task = Storage.Client.CreateDocumentWithRetriesAsync(Storage.CollectionUri, entityJob);
            task.Wait();

            if (task.Result.StatusCode == HttpStatusCode.Created || task.Result.StatusCode == HttpStatusCode.OK)
            {
                return entityJob.Id;
            }

            return string.Empty;
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0) throw new ArgumentNullException(nameof(queues));

            IPersistentJobQueueProvider[] providers = queues.Select(q => QueueProviders.GetProvider(q))
                .Distinct()
                .ToArray();

            if (providers.Length != 1)
            {
                throw new InvalidOperationException($"Multiple provider instances registered for queues: {string.Join(", ", queues)}. You should choose only one type of persistent queues per server instance.");
            }

            IPersistentJobQueue persistentQueue = providers.Single().GetJobQueue();
            IFetchedJob queue = persistentQueue.Dequeue(queues, cancellationToken);
            return queue;
        }

        public override JobData GetJobData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            Entities.Job data = Storage.Client.CreateDocumentQuery<Entities.Job>(Storage.CollectionUri, queryOptions)
                .Where(j => j.Id == jobId)
                .AsEnumerable()
                .FirstOrDefault();

            if (data != null)
            {
                InvocationData invocationData = data.InvocationData;
                invocationData.Arguments = data.Arguments;

                Common.Job job = null;
                JobLoadException loadException = null;

                try
                {
                    job = invocationData.Deserialize();
                }
                catch (JobLoadException ex)
                {
                    loadException = ex;
                }

                return new JobData
                {
                    Job = job,
                    State = data.StateName,
                    CreatedAt = data.CreatedOn,
                    LoadException = loadException
                };
            }

            return null;
        }

        public override StateData GetStateData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT TOP 1 * FROM c WHERE c.job_id = @jobId AND c.type = @type ORDER BY c.created_on DESC",
                Parameters = new SqlParameterCollection
                 {
                     new SqlParameter("@jobId", jobId),
                     new SqlParameter("@type", DocumentTypes.State)
                 }
            };

            State state = Storage.Client.CreateDocumentQuery<State>(Storage.CollectionUri, sql)
                 .FirstOrDefault();

            if (state != null)
            {
                return new StateData
                {
                    Name = state.Name,
                    Reason = state.Reason,
                    Data = state.Data
                };
            }

            return null;
        }

        #endregion

        #region Parameter

        public override string GetJobParameter(string id, string name)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            List<Parameter> parameters = Storage.Client.CreateDocumentQuery<Entities.Job>(Storage.CollectionUri, queryOptions)
                 .Where(j => j.Id == id)
                 .SelectMany(j => j.Parameters)
                 .AsEnumerable()
                 .ToList();

            return parameters.Where(p => p.Name == name).Select(p => p.Value).FirstOrDefault();
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            Uri spSetJobParameterUri = UriFactory.CreateStoredProcedureUri(Storage.Options.DatabaseName, Storage.Options.CollectionName, "setJobParameter");
            Task<StoredProcedureResponse<bool>> task = Storage.Client.ExecuteStoredProcedureAsync<bool>(spSetJobParameterUri, id, name, value);
            task.Wait();
        }

        #endregion

        #region Set

        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            DateTime? expireOn = Storage.Client.CreateDocumentQuery<Set>(Storage.CollectionUri, queryOptions)
                .Where(s => s.Key == key && s.DocumentType == DocumentTypes.Set)
                .Min(s => s.ExpireOn);

            return expireOn.HasValue ? expireOn.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Storage.Client.CreateDocumentQuery<Set>(Storage.CollectionUri, queryOptions)
                .Where(s => s.Key == key && s.DocumentType == DocumentTypes.Set)
                .Select(c => c.Value)
                .AsEnumerable()
                .Skip(startingFrom).Take(endingAt)
                .ToList();
        }

        public override long GetCounter(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Storage.Client.CreateDocumentQuery<Counter>(Storage.CollectionUri, queryOptions)
                .Where(c => c.Key == key && c.DocumentType == DocumentTypes.Counter)
                .Sum(c => c.Value);
        }

        public override long GetSetCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Storage.Client.CreateDocumentQuery<Set>(Storage.CollectionUri, queryOptions)
                .Where(s => s.Key == key && s.DocumentType == DocumentTypes.Set)
                .LongCount();
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            List<string> sets = Storage.Client.CreateDocumentQuery<Set>(Storage.CollectionUri, queryOptions)
                .Where(s => s.Key == key && s.DocumentType == DocumentTypes.Set)
                .Select(s => s.Value)
                .AsEnumerable()
                .ToList();

            return new HashSet<string>(sets);
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT TOP 1 VALUE c['value'] FROM c WHERE c.key = @key AND c.type = @type AND (c.score BETWEEN @from AND @to) " +
                            "ORDER BY c.created_on DESC ",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@key", key),
                    new SqlParameter("@type", DocumentTypes.State),
                    new SqlParameter("@from", fromScore ),
                    new SqlParameter("@to", toScore)
                }
            };

            return Storage.Client.CreateDocumentQuery<string>(Storage.CollectionUri, sql)
                .AsEnumerable()
                .FirstOrDefault();
        }

        #endregion

        #region Server

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));
            if (context == null) throw new ArgumentNullException(nameof(context));

            Entities.Server server = new Entities.Server
            {
                ServerId = serverId,
                Workers = context.WorkerCount,
                Queues = context.Queues,
                CreatedOn = DateTime.UtcNow,
                LastHeartbeat = DateTime.UtcNow
            };

            Uri spAnnounceServerUri = UriFactory.CreateStoredProcedureUri(Storage.Options.DatabaseName, Storage.Options.CollectionName, "announceServer");
            Task<StoredProcedureResponse<bool>> task = Storage.Client.ExecuteStoredProcedureAsync<bool>(spAnnounceServerUri, server);
            task.Wait();
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            Uri spHeartbeatServerUri = UriFactory.CreateStoredProcedureUri(Storage.Options.DatabaseName, Storage.Options.CollectionName, "heartbeatServer");
            Task<StoredProcedureResponse<bool>> task = Storage.Client.ExecuteStoredProcedureAsync<bool>(spHeartbeatServerUri, serverId, DateTime.UtcNow.ToEpoch());
            task.Wait();
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            Uri spRemoveServerUri = UriFactory.CreateStoredProcedureUri(Storage.Options.DatabaseName, Storage.Options.CollectionName, "removeServer");
            Task<StoredProcedureResponse<bool>> task = Storage.Client.ExecuteStoredProcedureAsync<bool>(spRemoveServerUri, serverId);
            task.Wait();
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException(@"invalid timeout", nameof(timeOut));
            }

            int lastHeartbeat = DateTime.UtcNow.Add(timeOut.Negate()).ToEpoch();

            Uri spRemovedTimeoutServerUri = UriFactory.CreateStoredProcedureUri(Storage.Options.DatabaseName, Storage.Options.CollectionName, "removedTimedOutServer");
            Task<StoredProcedureResponse<int>> task = Storage.Client.ExecuteStoredProcedureAsync<int>(spRemovedTimeoutServerUri, lastHeartbeat);
            task.Wait();
            return task.Result.Response;
        }

        #endregion

        #region Hash

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Storage.Client.CreateDocumentQuery<Hash>(Storage.CollectionUri, queryOptions)
                .Where(h => h.Key == key && h.DocumentType == DocumentTypes.Hash)
                .AsEnumerable()
                .ToDictionary(h => h.Field, h => h.Value);
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            Hash[] sources = keyValuePairs.Select(k => new Hash
            {
                Key = key,
                Field = k.Key,
                Value = k.Value.ToEpoch()
            }).ToArray();

            Uri spSetRangeHashUri = UriFactory.CreateStoredProcedureUri(Storage.Options.DatabaseName, Storage.Options.CollectionName, "setRangeHash");
            Task<StoredProcedureResponse<bool>> task = Storage.Client.ExecuteStoredProcedureAsync<bool>(spSetRangeHashUri, key, sources);
            task.Wait();
        }

        public override long GetHashCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Storage.Client.CreateDocumentQuery<Hash>(Storage.CollectionUri, queryOptions)
                .Where(h => h.Key == key && h.DocumentType == DocumentTypes.Hash)
                .LongCount();
        }

        public override string GetValueFromHash(string key, string name)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (name == null) throw new ArgumentNullException(nameof(name));

            return Storage.Client.CreateDocumentQuery<Hash>(Storage.CollectionUri, queryOptions)
                .Where(h => h.Key == key && h.Field == name && h.DocumentType == DocumentTypes.Hash)
                .Select(h => h.Value)
                .AsEnumerable()
                .FirstOrDefault();
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            DateTime? expireOn = Storage.Client.CreateDocumentQuery<Hash>(Storage.CollectionUri, queryOptions)
                .Where(h => h.Key == key && h.DocumentType == DocumentTypes.Hash)
                .Min(h => h.ExpireOn);

            return expireOn.HasValue ? expireOn.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        #endregion

        #region List

        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Storage.Client.CreateDocumentQuery<List>(Storage.CollectionUri, queryOptions)
                .Where(l => l.Key == key && l.DocumentType == DocumentTypes.List)
                .Select(l => l.Value)
                .AsEnumerable()
                .ToList();
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT VALUE c['value'] FROM c WHERE c.key = @key AND c.type = @type ORDER BY c.expire_on",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@key", key),
                    new SqlParameter("@type", DocumentTypes.List)
                }
            };

            return Storage.Client.CreateDocumentQuery<string>(Storage.CollectionUri, sql)
                .AsEnumerable()
                .Skip(startingFrom).Take(endingAt)
                .ToList();
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            DateTime? expireOn = Storage.Client.CreateDocumentQuery<List>(Storage.CollectionUri, queryOptions)
                .Where(l => l.Key == key && l.DocumentType == DocumentTypes.List)
                .Min(l => l.ExpireOn);

            return expireOn.HasValue ? expireOn.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override long GetListCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Storage.Client.CreateDocumentQuery<List>(Storage.CollectionUri, queryOptions)
                .Where(l => l.Key == key && l.DocumentType == DocumentTypes.List)
                .LongCount();
        }

        #endregion

    }
}