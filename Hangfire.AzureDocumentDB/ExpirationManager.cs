using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;

using Hangfire.Server;
using Hangfire.Logging;
using Hangfire.AzureDocumentDB.Entities;

namespace Hangfire.AzureDocumentDB
{
#pragma warning disable 618
    internal class ExpirationManager : IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog logger = LogProvider.For<ExpirationManager>();
        private const string DISTRIBUTED_LOCK_KEY = "expirationmanager";
        private static readonly TimeSpan defaultLockTimeout = TimeSpan.FromMinutes(5);
        private static readonly string[] documents = { "locks", "jobs", "lists", "sets", "hashes", "counters" };
        private readonly TimeSpan checkInterval;
        private readonly AzureDocumentDbStorage storage;
        private readonly Uri spDeleteExpiredDocumentsUri;

        public ExpirationManager(AzureDocumentDbStorage storage)
        {
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
            checkInterval = storage.Options.ExpirationCheckInterval;
            spDeleteExpiredDocumentsUri = UriFactory.CreateStoredProcedureUri(storage.Options.DatabaseName, storage.Options.CollectionName, "deleteExpiredDocuments");
        }

        public void Execute(CancellationToken cancellationToken)
        {
            foreach (string document in documents)
            {
                logger.Debug($"Removing outdated records from the '{document}' document.");
                DocumentTypes type = document.ToDocumentType();

                using (new AzureDocumentDbDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage))
                {
                    Task<StoredProcedureResponse<int>> procedureTask = storage.Client.ExecuteStoredProcedureAsync<int>(spDeleteExpiredDocumentsUri, type);
                    Task task =  procedureTask.ContinueWith(t => logger.Trace($"Outdated records removed {t.Result.Response} records from the '{document}' document."), cancellationToken);
                    task.Wait(cancellationToken);
                }

                cancellationToken.WaitHandle.WaitOne(checkInterval);
            }
        }
    }

    internal static class ExpirationManagerExtenison
    {
        internal static DocumentTypes ToDocumentType(this string document)
        {
            switch (document)
            {
                case "locks": return DocumentTypes.Lock;
                case "jobs": return DocumentTypes.Job;
                case "lists": return DocumentTypes.List;
                case "sets": return DocumentTypes.Set;
                case "hashes": return DocumentTypes.Hash;
                case "counters": return DocumentTypes.Counter;
            }

            throw new ArgumentException(@"invalid document type", nameof(document));
        }
    }
}