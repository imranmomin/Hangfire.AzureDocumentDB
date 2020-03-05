﻿using System;
using System.Collections.Generic;
using System.Linq;

using Hangfire.Azure.Documents;
using Hangfire.Azure.Helper;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.SystemFunctions;

namespace Hangfire.Azure.Queue
{
    internal class JobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly DocumentDbStorage storage;
        private readonly List<string> queuesCache = new List<string>();
        private DateTime cacheUpdated;
        private readonly object cacheLock = new object();
        private static readonly TimeSpan queuesCacheTimeout = TimeSpan.FromSeconds(5);
        private readonly PartitionKey partitionKey = new PartitionKey((int)DocumentTypes.Queue);

        public JobQueueMonitoringApi(DocumentDbStorage storage) => this.storage = storage;

        public IEnumerable<string> GetQueues()
        {
            lock (cacheLock)
            {
                if (queuesCache.Count == 0 || cacheUpdated.Add(queuesCacheTimeout) < DateTime.UtcNow)
                {
                    SqlQuerySpec sql = new SqlQuerySpec
                    {
                        QueryText = "SELECT VALUE doc['name'] FROM doc WHERE doc.type = @type",
                        Parameters = new SqlParameterCollection
                        {
                            new SqlParameter("@type", (int)DocumentTypes.Queue)
                        }
                    };

                    IEnumerable<string> result = storage.Client.CreateDocumentQuery<string>(storage.CollectionUri, sql, new FeedOptions { PartitionKey = partitionKey })
                        .ToQueryResult()
                        .Distinct();

                    queuesCache.Clear();
                    queuesCache.AddRange(result);
                    cacheUpdated = DateTime.UtcNow;
                }

                return queuesCache.ToList();
            }
        }

        public int GetEnqueuedCount(string queue)
        {
            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.type = @type AND doc.name = @name",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@name", queue),
                    new SqlParameter("@type", (int)DocumentTypes.Queue)
                }
            };

            return storage.Client.CreateDocumentQuery<int>(storage.CollectionUri, sql, new FeedOptions { PartitionKey = partitionKey })
                .ToQueryResult()
                .FirstOrDefault();
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            return storage.Client.CreateDocumentQuery<Documents.Queue>(storage.CollectionUri, new FeedOptions { PartitionKey = partitionKey })
                .Where(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue && q.FetchedAt.IsDefined() == false)
                .OrderBy(q => q.CreatedOn)
                .Skip(from).Take(perPage)
                .Select(q => q.JobId)
                .ToQueryResult();
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage)
        {
            return storage.Client.CreateDocumentQuery<Documents.Queue>(storage.CollectionUri, new FeedOptions { PartitionKey = partitionKey })
                .Where(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue && q.FetchedAt.IsDefined())
                .OrderBy(q => q.CreatedOn)
                .Skip(from).Take(perPage)
                .Select(q => q.JobId)
                .ToQueryResult();
        }

        public (int? EnqueuedCount, int? FetchedCount) GetEnqueuedAndFetchedCount(string queue)
        {

            (int EnqueuedCount, int FetchedCount) result = storage.Client.CreateDocumentQuery<Documents.Queue>(storage.CollectionUri, new FeedOptions { PartitionKey = partitionKey })
                .Where(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue)
                .Select(q => new { q.Name, EnqueuedCount = q.FetchedAt.IsDefined() ? 0 : 1, FetchedCount = q.FetchedAt.IsDefined() ? 1 : 0 })
                .ToQueryResult()
                .GroupBy(q => q.Name)
                .Select(v => (EnqueuedCount: v.Sum(q => q.EnqueuedCount), FetchedCount: v.Sum(q => q.FetchedCount)))
                .FirstOrDefault();

            return result;
        }

    }
}