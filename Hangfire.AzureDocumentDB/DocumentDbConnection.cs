﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Hangfire.Azure.Documents;
using Hangfire.Azure.Documents.Helper;
using Hangfire.Azure.Helper;
using Hangfire.Azure.Queue;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace Hangfire.Azure
{
    internal sealed class DocumentDbConnection : JobStorageConnection
    {
        public DocumentDbStorage Storage { get; }
        public PersistentJobQueueProviderCollection QueueProviders { get; }

        public DocumentDbConnection(DocumentDbStorage storage)
        {
            Storage = storage;
            QueueProviders = storage.QueueProviders;
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout) => new DocumentDbDistributedLock(resource, timeout, Storage);
        public override IWriteOnlyTransaction CreateWriteTransaction() => new DocumentDbWriteOnlyTransaction(this);

        #region Job

        public override string CreateExpiredJob(Common.Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException(nameof(job));
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));

            InvocationData invocationData = InvocationData.SerializeJob(job);
            Documents.Job entityJob = new Documents.Job
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

            Task<ResourceResponse<Document>> task = Storage.Client.CreateDocumentWithRetriesAsync(Storage.CollectionUri, entityJob, new RequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Job) });
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

            Uri uri = UriFactory.CreateDocumentUri(Storage.Options.DatabaseName, Storage.Options.CollectionName, jobId);
            Task<DocumentResponse<Documents.Job>> task = Storage.Client.ReadDocumentWithRetriesAsync<Documents.Job>(uri, new RequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Job) });
            task.Wait();

            if (task.Result.Document != null)
            {
                Documents.Job data = task.Result;
                InvocationData invocationData = data.InvocationData;
                invocationData.Arguments = data.Arguments;

                Common.Job job = null;
                JobLoadException loadException = null;

                try
                {
                    job = invocationData.DeserializeJob();
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

            Uri uri = UriFactory.CreateDocumentUri(Storage.Options.DatabaseName, Storage.Options.CollectionName, jobId);
            Task<DocumentResponse<Documents.Job>> task = Storage.Client.ReadDocumentWithRetriesAsync<Documents.Job>(uri, new RequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Job) });
            task.Wait();

            if (task.Result.Document != null)
            {
                Documents.Job job = task.Result;

                // get the state document
                uri = UriFactory.CreateDocumentUri(Storage.Options.DatabaseName, Storage.Options.CollectionName, job.StateId);
                Task<DocumentResponse<State>> stateTask = Storage.Client.ReadDocumentWithRetriesAsync<State>(uri, new RequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.State) });
                stateTask.Wait();

                if (stateTask.Result.Document != null)
                {
                    State state = stateTask.Result;
                    return new StateData
                    {
                        Name = state.Name,
                        Reason = state.Reason,
                        Data = state.Data
                    };
                }
            }

            return null;
        }

        #endregion

        #region Parameter

        public override string GetJobParameter(string id, string name)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            Uri uri = UriFactory.CreateDocumentUri(Storage.Options.DatabaseName, Storage.Options.CollectionName, id);
            Task<DocumentResponse<Documents.Job>> task = Storage.Client.ReadDocumentWithRetriesAsync<Documents.Job>(uri, new RequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Job) });
            Documents.Job data = task.Result;

            return data?.Parameters.Where(p => p.Name == name).Select(p => p.Value).FirstOrDefault();
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            Parameter parameter = new Parameter
            {
                Value = value,
                Name = name
            };

            Uri spSetJobParameterUri = UriFactory.CreateStoredProcedureUri(Storage.Options.DatabaseName, Storage.Options.CollectionName, "setJobParameter");
            Task<StoredProcedureResponse<bool>> task = Storage.Client.ExecuteStoredProcedureWithRetriesAsync<bool>(spSetJobParameterUri, new RequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Job) }, default, id, parameter);
            task.Wait();
        }

        #endregion

        #region Set

        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT TOP 1 VALUE MIN(doc['expire_on']) FROM doc WHERE doc.type = @type AND doc.key = @key",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@key", key),
                    new SqlParameter("@type", (int)DocumentTypes.Set)
                }
            };

            int? expireOn = Storage.Client.CreateDocumentQuery<int?>(Storage.CollectionUri, sql, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Set) })
                .ToQueryResult()
                .FirstOrDefault();

            return expireOn.HasValue ? expireOn.Value.ToDateTime() - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            endingAt += 1 - startingFrom;

            return Storage.Client.CreateDocumentQuery<Set>(Storage.CollectionUri, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Set) })
                .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key)
                .OrderBy(s => s.CreatedOn)
                .Skip(startingFrom).Take(endingAt)
                .Select(s => s.Value)
                .ToQueryResult()
                .ToList();
        }

        public override long GetCounter(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT TOP 1 VALUE SUM(doc['value']) FROM doc WHERE doc.type = @type AND doc.key = @key",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@key", key),
                    new SqlParameter("@type", (int)DocumentTypes.Counter)
                }
            };

            return Storage.Client.CreateDocumentQuery<long>(Storage.CollectionUri, sql, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Counter) })
                .ToQueryResult()
                .FirstOrDefault();
        }

        public override long GetSetCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.type = @type AND doc.key = @key",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@key", key),
                    new SqlParameter("@type",(int)DocumentTypes.Set)
                }
            };

            return Storage.Client.CreateDocumentQuery<long>(Storage.CollectionUri, sql, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Set) })
                .ToQueryResult()
                .FirstOrDefault();
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            IEnumerable<string> sets = Storage.Client.CreateDocumentQuery<Set>(Storage.CollectionUri, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Set) })
                .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key)
                .Select(s => s.Value)
                .ToQueryResult();

            return new HashSet<string>(sets);
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            return GetFirstByLowestScoreFromSet(key, fromScore, toScore, 1).FirstOrDefault();
        }

        public override List<string> GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore, int count)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (count <= 0) throw new ArgumentException("The value must be a positive number", nameof(count));
            if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

            return Storage.Client.CreateDocumentQuery<Set>(Storage.CollectionUri, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Set) })
                .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key && s.Score >= fromScore && s.Score <= toScore)
                .OrderBy(s => s.Score)
                .Take(count)
                .Select(s => s.Value)
                .ToQueryResult()
                .ToList();
        }

        #endregion

        #region Server

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));
            if (context == null) throw new ArgumentNullException(nameof(context));

            Documents.Server server = new Documents.Server
            {
                Id = $"{serverId}:{DocumentTypes.Server}".GenerateHash(),
                ServerId = serverId,
                Workers = context.WorkerCount,
                Queues = context.Queues,
                CreatedOn = DateTime.UtcNow,
                LastHeartbeat = DateTime.UtcNow
            };
            Task<ResourceResponse<Document>> task = Storage.Client.UpsertDocumentWithRetriesAsync(Storage.CollectionUri, server, new RequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Server) });
            task.Wait();
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));
            string id = $"{serverId}:{DocumentTypes.Server}".GenerateHash();

            Uri spHeartbeatServerUri = UriFactory.CreateStoredProcedureUri(Storage.Options.DatabaseName, Storage.Options.CollectionName, "heartbeatServer");
            Task<StoredProcedureResponse<bool>> task = Storage.Client.ExecuteStoredProcedureWithRetriesAsync<bool>(spHeartbeatServerUri, new RequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Server) }, default, id, DateTime.UtcNow.ToEpoch());
            task.Wait();
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));
            string id = $"{serverId}:{DocumentTypes.Server}".GenerateHash();

            Uri documentUri = UriFactory.CreateDocumentUri(Storage.Options.DatabaseName, Storage.Options.CollectionName, id);
            Task<ResourceResponse<Document>> task = Storage.Client.DeleteDocumentWithRetriesAsync(documentUri, new RequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Server) });
            task.Wait();
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException(@"invalid timeout", nameof(timeOut));
            }

            int lastHeartbeat = DateTime.UtcNow.Add(timeOut.Negate()).ToEpoch();
            string query = $"SELECT doc._self FROM doc WHERE doc.type = {(int)DocumentTypes.Server} AND IS_DEFINED(doc.last_heartbeat) " +
                           $"AND doc.last_heartbeat <= {lastHeartbeat}";

            return Storage.Client.ExecuteDeleteDocuments(query, new RequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Server) });
        }

        #endregion

        #region Hash

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Storage.Client.CreateDocumentQuery<Hash>(Storage.CollectionUri, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Hash) })
                .Where(h => h.DocumentType == DocumentTypes.Hash && h.Key == key)
                .Select(h => new { h.Field, h.Value })
                .ToQueryResult()
                .ToDictionary(h => h.Field, h => h.Value);
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            Data<Hash> data = new Data<Hash>();

            PartitionKey partitionKey = new PartitionKey((int)DocumentTypes.Hash);
            List<Hash> hashes = Storage.Client.CreateDocumentQuery<Hash>(Storage.CollectionUri, new FeedOptions { PartitionKey = partitionKey })
                .Where(h => h.DocumentType == DocumentTypes.Hash && h.Key == key)
                .ToQueryResult()
                .ToList();

            Hash[] sources = keyValuePairs.Select(k => new Hash
            {
                Key = key,
                Field = k.Key,
                Value = k.Value.TryParseToEpoch()
            }).ToArray();

            foreach (Hash source in sources)
            {
                Hash hash = hashes.SingleOrDefault(h => h.Field == source.Field);
                if (hash == null)
                {
                    data.Items.Add(source);
                }
                else if (!string.Equals(hash.Value, source.Value, StringComparison.InvariantCultureIgnoreCase))
                {
                    hash.Value = source.Value;
                    data.Items.Add(hash);
                }
            }

            Storage.Client.ExecuteUpsertDocuments(data, new RequestOptions { PartitionKey = partitionKey });
        }

        public override long GetHashCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.type = @type AND doc.key = @key",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@key", key),
                    new SqlParameter("@type", (int)DocumentTypes.Hash)
                }
            };

            return Storage.Client.CreateDocumentQuery<long>(Storage.CollectionUri, sql, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Hash) })
                .ToQueryResult()
                .FirstOrDefault();
        }

        public override string GetValueFromHash(string key, string name)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (name == null) throw new ArgumentNullException(nameof(name));

            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT TOP 1 VALUE doc['value'] FROM doc WHERE doc.type = @type AND doc.key = @key AND doc.field = @field",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@key", key),
                    new SqlParameter("@field", name),
                    new SqlParameter("@type", (int)DocumentTypes.Hash)
                }
            };

            return Storage.Client.CreateDocumentQuery<string>(Storage.CollectionUri, sql, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Hash) })
                .ToQueryResult()
                .FirstOrDefault();
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT TOP 1 VALUE MIN(doc['expire_on']) FROM doc WHERE doc.type = @type AND doc.key = @key ",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@key", key),
                    new SqlParameter("@type", (int)DocumentTypes.Hash)
                }
            };

            int? expireOn = Storage.Client.CreateDocumentQuery<int?>(Storage.CollectionUri, sql, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Hash) })
                .ToQueryResult()
                .FirstOrDefault();

            return expireOn.HasValue ? expireOn.Value.ToDateTime() - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        #endregion

        #region List

        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Storage.Client.CreateDocumentQuery<List>(Storage.CollectionUri, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.List) })
                .Where(l => l.DocumentType == DocumentTypes.List && l.Key == key)
                .OrderByDescending(l => l.CreatedOn)
                .Select(l => l.Value)
                .ToQueryResult()
                .ToList();
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            endingAt += 1 - startingFrom;

            return Storage.Client.CreateDocumentQuery<List>(Storage.CollectionUri, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.List) })
                .Where(l => l.DocumentType == DocumentTypes.List && l.Key == key)
                .OrderByDescending(l => l.CreatedOn)
                .Skip(startingFrom).Take(endingAt)
                .Select(l => l.Value)
                .ToQueryResult()
                .ToList();
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT TOP 1 VALUE MIN(doc['expire_on']) FROM doc WHERE doc.type = @type AND doc.key = @key",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@key", key),
                    new SqlParameter("@type", (int)DocumentTypes.List)
                }
            };

            int? expireOn = Storage.Client.CreateDocumentQuery<int?>(Storage.CollectionUri, sql, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.List) })
                .ToQueryResult()
                .FirstOrDefault();

            return expireOn.HasValue ? expireOn.Value.ToDateTime() - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override long GetListCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.type = @type AND doc.key = @key",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@key", key),
                    new SqlParameter("@type",(int)DocumentTypes.List)
                }
            };

            return Storage.Client.CreateDocumentQuery<long>(Storage.CollectionUri, sql, new FeedOptions { PartitionKey = new PartitionKey((int)DocumentTypes.List) })
                .ToQueryResult()
                .FirstOrDefault();
        }

        #endregion

    }
}