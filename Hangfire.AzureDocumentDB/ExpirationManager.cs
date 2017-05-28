using System;
using System.Threading;

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
            if (storage == null) throw new ArgumentNullException(nameof(storage));

            this.storage = storage;
            checkInterval = storage.Options.ExpirationCheckInterval;
            spDeleteExpiredDocumentsUri = UriFactory.CreateStoredProcedureUri(storage.Options.DatabaseName, storage.Options.CollectionName, "deleteExpiredDocuments");
        }

        readonly Func<DateTime, int> epoch = date =>
        {
            if (date.Equals(DateTime.MinValue)) return int.MinValue;
            DateTime epochDateTime = new DateTime(1970, 1, 1);
            TimeSpan epochTimeSpan = date - epochDateTime;
            return (int)epochTimeSpan.TotalSeconds;
        };

        public void Execute(CancellationToken cancellationToken)
        {
            int epochDate = epoch(DateTime.UtcNow);
            foreach (string document in documents)
            {
                logger.Debug($"Removing outdated records from the '{document}' document.");
                DocumentTypes type = document.ToDocumentType();

                using (new AzureDocumentDbDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage))
                {
                    StoredProcedureResponse<int> result = storage.Client.ExecuteStoredProcedureAsync<int>(spDeleteExpiredDocumentsUri, type, epochDate).GetAwaiter().GetResult();
                    logger.Trace($"Outdated records removed {result.Response} records from the '{document}' document.");
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