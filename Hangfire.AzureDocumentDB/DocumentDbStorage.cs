﻿using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;

using Hangfire.Server;
using Hangfire.Storage;
using Hangfire.Logging;
using Newtonsoft.Json;
using Hangfire.Azure.Queue;
using Microsoft.Azure.Documents;
using Hangfire.Azure.Documents.Json;
using Microsoft.Azure.Documents.Client;
using System.Reflection;

namespace Hangfire.Azure
{
    /// <summary>
    /// DocumentDbStorage extend the storage option for Hangfire.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    public sealed class DocumentDbStorage : JobStorage
    {
        internal DocumentDbStorageOptions Options { get; }

        internal PersistentJobQueueProviderCollection QueueProviders { get; }

        internal DocumentClient Client { get; }

        internal Uri CollectionUri { get; private set; }

        /// <summary>
        /// Initializes the DocumentDbStorage form the url auth secret provide.
        /// </summary>
        /// <param name="url">The url string to DocumentDb Database</param>
        /// <param name="authSecret">The secret key for the DocumentDb Database</param>
        /// <param name="database">The name of the database to connect with</param>
        /// <param name="collection">The name of the collection on the database</param>
        /// <exception cref="ArgumentNullException"><paramref name="url"/> argument is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="authSecret"/> argument is null.</exception>
        public DocumentDbStorage(string url, string authSecret, string database, string collection) : this(new DocumentDbStorageOptions { Endpoint = new Uri(url), AuthSecret = authSecret, DatabaseName = database, CollectionName = collection }) { }

        /// <summary>
        /// Initializes the DocumentDbStorage form the url auth secret provide.
        /// </summary>
        /// <param name="url">The url string to DocumentDb Database</param>
        /// <param name="authSecret">The secret key for the DocumentDb Database</param>
        /// <param name="database">The name of the database to connect with</param>
        /// <param name="options">The DocumentDbStorageOptions object to override any of the options</param>
        /// <param name="collection">The name of the collection on the database</param>
        /// <exception cref="ArgumentNullException"><paramref name="url"/> argument is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="authSecret"/> argument is null.</exception>
        public DocumentDbStorage(string url, string authSecret, string database, string collection, DocumentDbStorageOptions options) : this(Transform(url, authSecret, database, collection, options)) { }

        /// <summary>
        /// Initializes the DocumentDbStorage form the url auth secret provide.
        /// </summary>
        /// <param name="options">The DocumentDbStorageOptions object to override any of the options</param>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> argument is null.</exception>
        private DocumentDbStorage(DocumentDbStorageOptions options)
        {
            Options = options ?? throw new ArgumentNullException(nameof(options));

            JsonSerializerSettings settings = new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                DateTimeZoneHandling = DateTimeZoneHandling.Utc,
                ContractResolver = new DocumentContractResolver()
            };

            ConnectionPolicy connectionPolicy = ConnectionPolicy.Default;
            connectionPolicy.RequestTimeout = options.RequestTimeout;
            Client = new DocumentClient(options.Endpoint, options.AuthSecret, settings, connectionPolicy);
            Task task = Client.OpenAsync();
            Task continueTask = task.ContinueWith(t => Initialize(), TaskContinuationOptions.OnlyOnRanToCompletion);
            continueTask.Wait();

            JobQueueProvider provider = new JobQueueProvider(this);
            QueueProviders = new PersistentJobQueueProviderCollection(provider);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override IStorageConnection GetConnection() => new DocumentDbConnection(this);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override IMonitoringApi GetMonitoringApi() => new DocumentDbMonitoringApi(this);

#pragma warning disable 618
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore 618
        {
            yield return new ExpirationManager(this);
            yield return new CountersAggregator(this);
        }

        /// <summary>
        /// Prints out the storage options
        /// </summary>
        /// <param name="logger"></param>
        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for Azure DocumentDB job storage:");
            logger.Info($"     DocumentDB Url: {Options.Endpoint.AbsoluteUri}");
            logger.Info($"     Request Timeout: {Options.RequestTimeout}");
            logger.Info($"     Counter Agggerate Interval: {Options.CountersAggregateInterval.TotalSeconds} seconds");
            logger.Info($"     Queue Poll Interval: {Options.QueuePollInterval.TotalSeconds} seconds");
            logger.Info($"     Expiration Check Interval: {Options.ExpirationCheckInterval.TotalSeconds} seconds");
            logger.Info($"     Queue: {string.Join(",", Options.Queues)}");
        }

        /// <summary>
        /// Return the name of the database
        /// </summary>
        /// <returns></returns>
        public override string ToString() => $"DoucmentDb Database : {Options.DatabaseName}";

        private void Initialize()
        {
            ILog logger = LogProvider.For<DocumentDbStorage>();

            // create database
            logger.Info($"Creating database : {Options.DatabaseName}");
            Task databaseTask = Client.CreateDatabaseIfNotExistsAsync(new Database { Id = Options.DatabaseName });

            // create document collection
            Task collectionTask = databaseTask.ContinueWith(t =>
            {
                 logger.Info($"Creating document collection : {Options.CollectionName}");
                 Uri databaseUri = UriFactory.CreateDatabaseUri(Options.DatabaseName);
                 return Client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, new DocumentCollection { Id = Options.CollectionName });
            }, TaskContinuationOptions.OnlyOnRanToCompletion);

            // create stored procedures 
            Task continueTask = collectionTask.ContinueWith(t =>
            {
                CollectionUri = UriFactory.CreateDocumentCollectionUri(Options.DatabaseName, Options.CollectionName);
                System.Reflection.Assembly assembly = typeof(DocumentDbStorage).GetTypeInfo().Assembly;
                string[] storedProcedureFiles = assembly.GetManifestResourceNames().Where(n => n.EndsWith(".js")).ToArray();
                foreach (string storedProcedureFile in storedProcedureFiles)
                {
                    logger.Info($"Creating database : {storedProcedureFile}");
                    using (Stream stream = assembly.GetManifestResourceStream(storedProcedureFile))
                    using (MemoryStream memoryStream = new MemoryStream())
                    {
                        stream?.CopyTo(memoryStream);
                        StoredProcedure sp = new StoredProcedure
                        {
                            Body = Encoding.UTF8.GetString(memoryStream.ToArray()),
                            Id = Path.GetFileNameWithoutExtension(storedProcedureFile)?
                                .Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries)
                                .Last()
                        };
                        Client.UpsertStoredProcedureAsync(CollectionUri, sp);
                    }
                }
            }, TaskContinuationOptions.OnlyOnRanToCompletion);

            // wait for 10 seconds, before timeout;
            continueTask.Wait(10000);
        }

        private static DocumentDbStorageOptions Transform(string url, string authSecret, string database, string collection, DocumentDbStorageOptions options)
        {
            if (options == null) options = new DocumentDbStorageOptions();

            options.Endpoint = new Uri(url);
            options.AuthSecret = authSecret;
            options.DatabaseName = database;
            options.CollectionName = collection;

            return options;
        }
    }
}
