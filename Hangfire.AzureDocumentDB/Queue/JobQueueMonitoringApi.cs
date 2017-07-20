using System.Linq;
using System.Collections.Generic;

using Hangfire.Azure.Documents;

namespace Hangfire.Azure.Queue
{
    internal class JobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly DocumentDbStorage storage;
        private readonly IEnumerable<string> queues;

        public JobQueueMonitoringApi(DocumentDbStorage storage)
        {
            this.storage = storage;
            queues = storage.Options.Queues;
        }

        public IEnumerable<string> GetQueues() => queues;

        public int GetEnqueuedCount(string queue)
        {
            return storage.Client.CreateDocumentQuery<Documents.Queue>(storage.CollectionUri)
                .Count(q => q.Name == queue && q.DocumentType == DocumentTypes.Queue);
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            return storage.Client.CreateDocumentQuery<Documents.Queue>(storage.CollectionUri)
                .Where(q => q.Name == queue && q.DocumentType == DocumentTypes.Queue)
                .Select(c => c.JobId)
                .AsEnumerable()
                .Skip(from).Take(perPage)
                .ToList();
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage) => GetEnqueuedJobIds(queue, from, perPage);

    }
}