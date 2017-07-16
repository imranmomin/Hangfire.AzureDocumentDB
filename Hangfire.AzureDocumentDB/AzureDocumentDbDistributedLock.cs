using System;
using System.Net;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Azure.Documents;
using Hangfire.AzureDocumentDB.Helper;
using Microsoft.Azure.Documents.Client;
using Hangfire.AzureDocumentDB.Entities;

namespace Hangfire.AzureDocumentDB
{
    internal class AzureDocumentDbDistributedLock : IDisposable
    {
        private readonly AzureDocumentDbStorage storage;
        private string resourceId;
        private readonly object syncLock = new object();

        public AzureDocumentDbDistributedLock(string resource, TimeSpan timeout, AzureDocumentDbStorage storage)
        {
            this.storage = storage;
            Acquire(resource, timeout);
        }

        public void Dispose() => Relase();

        private void Acquire(string name, TimeSpan timeout)
        {
            System.Diagnostics.Stopwatch acquireStart = new System.Diagnostics.Stopwatch();
            acquireStart.Start();

            while (string.IsNullOrEmpty(resourceId))
            {
                SqlQuerySpec sql = new SqlQuerySpec
                {
                    QueryText = "SELECT TOP 1 1 FROM c WHERE c.name = @name AND c.type = @type",
                    Parameters = new SqlParameterCollection
                    {
                        new SqlParameter("@name", name),
                        new SqlParameter("@type", DocumentTypes.Lock),
                    }
                };

                bool exists = storage.Client.CreateDocumentQuery(storage.CollectionUri, sql).AsEnumerable().Any();

                if (exists == false)
                {
                    Lock @lock = new Lock { Name = name, ExpireOn = DateTime.UtcNow.Add(timeout) };
                    Task<ResourceResponse<Document>> task = storage.Client.CreateDocumentWithRetriesAsync(storage.CollectionUri, @lock);
                    Task continueTask = task.ContinueWith(t =>
                    {
                        ResourceResponse<Document> response = t.Result;
                        if (response.StatusCode == HttpStatusCode.Created)
                        {
                            resourceId = @lock.Id;
                        }
                    });
                    continueTask.Wait();
                }

                // check the timeout
                if (acquireStart.ElapsedMilliseconds > timeout.TotalMilliseconds)
                {
                    throw new AzureDocumentDbDistributedLockException($"Could not place a lock on the resource '{name}': Lock timeout.");
                }

                // sleep for 1000 millisecond
                System.Threading.Thread.Sleep(1000);
            }
        }

        private void Relase()
        {
            if (!string.IsNullOrEmpty(resourceId))
            {
                lock (syncLock)
                {
                    Uri spDeleteDocumentIfExists = UriFactory.CreateStoredProcedureUri(storage.Options.DatabaseName, storage.Options.CollectionName, "deleteDocumentIfExists");
                    Task<string> task = storage.Client.ExecuteStoredProcedureAsync<bool>(spDeleteDocumentIfExists, resourceId).ContinueWith(t => resourceId = string.Empty);
                    task.Wait();
                }
            }
        }
    }
}