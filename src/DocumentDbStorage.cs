﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Hangfire.Azure.Documents.Json;
using Hangfire.Azure.Helper;
using Hangfire.Azure.Queue;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

using Newtonsoft.Json;


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
        /// <param name="options">The DocumentDbStorageOptions object to override any of the options</param>
        public DocumentDbStorage(string url, string authSecret, string database, string collection, DocumentDbStorageOptions options = null)
        {
            Options = options ?? new DocumentDbStorageOptions();
            Options.DatabaseName = database;
            Options.CollectionName = collection;

            // set the partitioning flag on the client helper
            ClientHelper.enablePartition = Options.EnablePartition;

            JsonSerializerSettings settings = new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                DateTimeZoneHandling = DateTimeZoneHandling.Utc,
                ContractResolver = new DocumentContractResolver()
            };

            ConnectionPolicy connectionPolicy = ConnectionPolicy.Default;
            connectionPolicy.ConnectionMode = Options.ConnectionMode;
            connectionPolicy.ConnectionProtocol = Options.ConnectionProtocol;
            connectionPolicy.RequestTimeout = Options.RequestTimeout;
            connectionPolicy.RetryOptions = new RetryOptions
            {
                MaxRetryWaitTimeInSeconds = 10,
                MaxRetryAttemptsOnThrottledRequests = 5
            };

            Client = new DocumentClient(new Uri(url), authSecret, settings, connectionPolicy);
            Task task = Client.OpenAsync();
            Task continueTask = task.ContinueWith(t => Initialize(), TaskContinuationOptions.OnlyOnRanToCompletion);
            continueTask.Wait();

            StoredprocedureHelper.Setup(database, collection);

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
            logger.Info($"     DocumentDB Url: {Client.ServiceEndpoint.AbsoluteUri}");
            logger.Info($"     Request Timeout: {Options.RequestTimeout}");
            logger.Info($"     Counter Aggregate Interval: {Options.CountersAggregateInterval.TotalSeconds} seconds");
            logger.Info($"     Queue Poll Interval: {Options.QueuePollInterval.TotalSeconds} seconds");
            logger.Info($"     Expiration Check Interval: {Options.ExpirationCheckInterval.TotalSeconds} seconds");
            logger.Info($"     Partition Enabled: {Options.EnablePartition}");
        }

        /// <summary>
        /// Return the name of the database
        /// </summary>
        /// <returns></returns>
        public override string ToString() => $"DoucmentDb : {Options.DatabaseName}";

        private void Initialize()
        {
            ILog logger = LogProvider.For<DocumentDbStorage>();

            // create database
            logger.Info($"Creating database : {Options.DatabaseName}");
            Task<ResourceResponse<Database>> databaseTask = Client.CreateDatabaseIfNotExistsAsync(new Database { Id = Options.DatabaseName });

            // create document collection
            Task<ResourceResponse<DocumentCollection>> collectionTask = databaseTask.ContinueWith(t =>
            {
                logger.Info($"Creating document collection : {t.Result.Resource.Id}");
                Uri databaseUri = UriFactory.CreateDatabaseUri(t.Result.Resource.Id);
                DocumentCollection documentCollection = new DocumentCollection { Id = Options.CollectionName };

                // if the partition option is enable
                if (Options.EnablePartition)
                {
                    documentCollection.PartitionKey = new PartitionKeyDefinition { Paths = new System.Collections.ObjectModel.Collection<string> { "/type" } };
                }

                return Client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, documentCollection);
            }, TaskContinuationOptions.OnlyOnRanToCompletion).Unwrap();

            // create stored procedures 
            Task continueTask = collectionTask.ContinueWith(t =>
            {
                CollectionUri = UriFactory.CreateDocumentCollectionUri(Options.DatabaseName, t.Result.Resource.Id);
                System.Reflection.Assembly assembly = System.Reflection.Assembly.GetExecutingAssembly();
                string[] storedProcedureFiles = assembly.GetManifestResourceNames().Where(n => n.EndsWith(".js")).ToArray();
                foreach (string storedProcedureFile in storedProcedureFiles)
                {
                    logger.Info($"Creating storedprocedure : {storedProcedureFile}");
                    Stream stream = assembly.GetManifestResourceStream(storedProcedureFile);
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

                        Uri storedProcedureUri = UriFactory.CreateStoredProcedureUri(Options.DatabaseName, t.Result.Resource.Id, sp.Id);
                        Task<ResourceResponse<StoredProcedure>> spTask = Client.ReplaceStoredProcedureAsync(storedProcedureUri, sp);
                        spTask.ContinueWith(x =>
                        {
                            if (x.Status == TaskStatus.Faulted && x.Exception.InnerException is DocumentClientException ex && ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                            {
                                return Client.CreateStoredProcedureAsync(CollectionUri, sp);
                            }

                            return Task.FromResult(x.Result);
                        }).Unwrap().Wait();
                    }
                    stream?.Close();
                }
            }, TaskContinuationOptions.OnlyOnRanToCompletion);

            continueTask.Wait();
            if (continueTask.IsFaulted || continueTask.IsCanceled)
            {
                throw new ApplicationException("Unable to create the stored procedures", databaseTask.Exception);
            }
        }
    }
}
