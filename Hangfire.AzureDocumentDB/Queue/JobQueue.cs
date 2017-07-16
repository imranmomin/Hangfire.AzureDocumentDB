using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;

using Hangfire.Storage;
using Hangfire.AzureDocumentDB.Helper;
using Microsoft.Azure.Documents;

namespace Hangfire.AzureDocumentDB.Queue
{
    internal class JobQueue : IPersistentJobQueue
    {
        private readonly AzureDocumentDbStorage storage;
        private const string DISTRIBUTED_LOCK_KEY = "locks:job:dequeue";
        private readonly TimeSpan defaultLockTimeout = TimeSpan.FromMinutes(1);
        private readonly TimeSpan checkInterval;
        private readonly object syncLock = new object();
        private readonly FeedOptions queryOptions = new FeedOptions { MaxItemCount = 1 };
        private readonly Uri spDeleteDocumentIfExistsUri;

        public JobQueue(AzureDocumentDbStorage storage)
        {
            this.storage = storage;
            checkInterval = storage.Options.QueuePollInterval;
            spDeleteDocumentIfExistsUri = UriFactory.CreateStoredProcedureUri(storage.Options.DatabaseName, storage.Options.CollectionName, "deleteDocumentIfExists");
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            int index = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                lock (syncLock)
                {
                    using (new AzureDocumentDbDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage))
                    {
                        string queue = queues.ElementAt(index);

                        Entities.Queue data = storage.Client.CreateDocumentQuery<Entities.Queue>(storage.CollectionUri, queryOptions)
                            .Where(q => q.Name == queue && q.DocumentType == Entities.DocumentTypes.Queue)
                            .AsEnumerable()
                            .FirstOrDefault();

                        if (data != null)
                        {
                            Task<StoredProcedureResponse<bool>> task = storage.Client.ExecuteStoredProcedureAsync<bool>(spDeleteDocumentIfExistsUri, data.Id);
                            task.Wait(cancellationToken);

                            if (task.Result.Response)
                            {
                                return new FetchedJob(storage, data);
                            }
                        }
                    }
                }

                Thread.Sleep(checkInterval);
                index = (index + 1) % queues.Length;
            }
        }

        public void Enqueue(string queue, string jobId)
        {
            Entities.Queue data = new Entities.Queue
            {
                Name = queue,
                JobId = jobId
            };

            Task<ResourceResponse<Document>> task = storage.Client.CreateDocumentWithRetriesAsync(storage.CollectionUri, data);
            task.Wait();
        }
    }
}