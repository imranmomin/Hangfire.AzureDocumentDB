using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace Hangfire.Azure.Helper
{
    internal static class ClientHelper
    {
        public static bool enablePartition;

        /// <summary>
        /// Extension method to create a query for documents in the Azure Cosmos DB service
        /// </summary>
        /// <typeparam name="T">the type of object to query</typeparam>
        /// <param name="client"></param>
        /// <param name="documentCollectionUri">The URI of the document collection.</param>
        /// <param name="feedOptions">The options for processing the query results feed</param>
        /// <returns></returns>
        public static IOrderedQueryable<T> CreateDocumentQueryAsync<T>(this DocumentClient client, Uri documentCollectionUri, FeedOptions feedOptions = null)
        {
            if (enablePartition == false && feedOptions != null)
            {
                feedOptions.PartitionKey = null;
                feedOptions.EnableCrossPartitionQuery = true;
            }

            return client.CreateDocumentQuery<T>(documentCollectionUri, feedOptions);
        }

        /// <summary>
        /// Extension method to create a query for documents in the Azure Cosmos DB service
        /// </summary>
        /// <typeparam name="T">the type of object to query</typeparam>
        /// <param name="client"></param>
        /// <param name="documentCollectionUri">The URI of the document collection.</param>
        /// <param name="feedOptions">The options for processing the query results feed</param>
        /// <returns></returns>
        public static IQueryable<T> CreateDocumentQueryAsync<T>(this DocumentClient client, Uri documentCollectionOrDatabaseUri, SqlQuerySpec querySpec, FeedOptions feedOptions = null)
        {
            if (enablePartition == false && feedOptions != null)
            {
                feedOptions.PartitionKey = null;
                feedOptions.EnableCrossPartitionQuery = true;
            }

            return client.CreateDocumentQuery<T>(documentCollectionOrDatabaseUri, querySpec, feedOptions);
        }


        /// <summary>
        /// Creates a document as an asynchronous operation in the Azure Cosmos DB service.
        /// </summary>
        /// <param name="client"></param>
        /// <param name="documentCollectionUri">the URI of the document collection to create the document in.</param>
        /// <param name="document">the document object.</param>
        /// <param name="options">The request options for the request.</param>
        /// <param name="disableAutomaticIdGeneration">Disables the automatic id generation, will throw an exception if id is missing.</param>
        /// <param name="cancellationToken">(Optional) <see cref="T:System.Threading.CancellationToken" /> representing request cancellation.</param>
        /// <returns></returns>
        internal static Task<ResourceResponse<Document>> CreateDocumentWithRetriesAsync(this DocumentClient client, Uri documentCollectionUri, object document, RequestOptions options = null, bool disableAutomaticIdGeneration = true, CancellationToken cancellationToken = default)
        {
            if (enablePartition == false && options != null)
            {
                options.PartitionKey = null;
            }

            return Task.Run(async () => await client.ExecuteWithRetries(x => x.CreateDocumentAsync(documentCollectionUri, document, options, disableAutomaticIdGeneration, cancellationToken)), cancellationToken);
        }

        /// <summary>
        /// Reads a <see cref="T:Microsoft.Azure.Documents.Document" /> as a generic type T from the Azure Cosmos DB service as an asynchronous operation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="client"></param>
        /// <param name="documentUri">A URI to the Document resource to be read.</param>
        /// <param name="options">The request options for the request.</param>
        /// <param name="cancellationToken">(Optional) <see cref="T:System.Threading.CancellationToken" /> representing request cancellation.</param>
        /// <returns></returns>
        internal static Task<DocumentResponse<T>> ReadDocumentWithRetriesAsync<T>(this DocumentClient client, Uri documentUri, RequestOptions options = null, CancellationToken cancellationToken = default)
        {
            if (enablePartition == false && options != null)
            {
                options.PartitionKey = null;
            }

            return Task.Run(async () => await client.ExecuteWithRetries(x => x.ReadDocumentAsync<T>(documentUri, options, cancellationToken)), cancellationToken);
        }

        /// <summary>
        /// Upserts a document as an asynchronous operation in the Azure Cosmos DB service.
        /// </summary>
        /// <param name="client"></param>
        /// <param name="documentCollectionUri">the URI of the document collection to upsert the document in.</param>
        /// <param name="document">the document object.</param>
        /// <param name="options">The request options for the request.</param>
        /// <param name="disableAutomaticIdGeneration">Disables the automatic id generation, will throw an exception if id is missing.</param>
        /// <param name="cancellationToken">(Optional) <see cref="T:System.Threading.CancellationToken" /> representing request cancellation.</param>
        internal static Task<ResourceResponse<Document>> UpsertDocumentWithRetriesAsync(this DocumentClient client, Uri documentCollectionUri, object document, RequestOptions options = null, bool disableAutomaticIdGeneration = false, CancellationToken cancellationToken = default)
        {
            if (enablePartition == false && options != null)
            {
                options.PartitionKey = null;
            }

            return Task.Run(async () => await client.ExecuteWithRetries(x => x.UpsertDocumentAsync(documentCollectionUri, document, options, disableAutomaticIdGeneration, cancellationToken)), cancellationToken);
        }

        /// <summary>
        /// Delete a document as an asynchronous operation from the Azure Cosmos DB service.
        /// </summary>
        /// <param name="client"></param>
        /// <param name="documentUri">the URI of the document to delete.</param>
        /// <param name="options">The request options for the request.</param>
        /// <param name="cancellationToken">(Optional) <see cref="T:System.Threading.CancellationToken" /> representing request cancellation.</param>
        internal static Task<ResourceResponse<Document>> DeleteDocumentWithRetriesAsync(this DocumentClient client, Uri documentUri, RequestOptions options = null, CancellationToken cancellationToken = default)
        {
            if (enablePartition == false && options != null)
            {
                options.PartitionKey = null;
            }

            return Task.Run(async () => await client.ExecuteWithRetries(x => x.DeleteDocumentAsync(documentUri, options, cancellationToken)), cancellationToken);
        }

        /// <summary>
        /// Replaces a document as an asynchronous operation in the Azure Cosmos DB service.
        /// </summary>
        /// <param name="client"></param>
        /// <param name="documentUri">the URI of the document to be updated.</param>
        /// <param name="document">the updated document.</param>
        /// <param name="options">The request options for the request.</param>
        /// <param name="cancellationToken">(Optional) <see cref="T:System.Threading.CancellationToken" /> representing request cancellation.</param>
        /// <returns></returns>
        internal static Task<ResourceResponse<Document>> ReplaceDocumentWithRetriesAsync(this DocumentClient client, Uri documentUri, object document, RequestOptions options = null, CancellationToken cancellationToken = default)
        {
            if (enablePartition == false && options != null)
            {
                options.PartitionKey = null;
            }

            return Task.Run(async () => await client.ExecuteWithRetries(x => x.ReplaceDocumentAsync(documentUri, document, options, cancellationToken)), cancellationToken);
        }

        /// <summary>
        /// Executes a stored procedure against a collection as an asynchronous operation from the Azure Cosmos DB service.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="client"></param>
        /// <param name="storedProcedureUri">the URI of the stored procedure to be executed.</param>
        /// <param name="procedureParams">the parameters for the stored procedure execution.</param>
        /// <returns></returns>
        internal static Task<StoredProcedureResponse<T>> ExecuteStoredProcedureWithRetriesAsync<T>(this DocumentClient client, Uri storedProcedureUri, RequestOptions options = null, CancellationToken cancellationToken = default, params object[] procedureParams)
        {
            if (enablePartition == false && options != null)
            {
                options.PartitionKey = null;
            }

            return Task.Run(async () => await client.ExecuteWithRetries(x => x.ExecuteStoredProcedureAsync<T>(storedProcedureUri, options, cancellationToken, procedureParams)));
        }

        /// <summary>
        /// Execute the function with retries on throttle
        /// </summary>
        internal static async Task<T> ExecuteWithRetries<T>(this DocumentClient client, Func<DocumentClient, Task<T>> function)
        {
            while (true)
            {
                TimeSpan timeSpan;

                try
                {
                    return await function(client);
                }
                catch (DocumentClientException ex) when (ex.StatusCode != null && (int)ex.StatusCode == 429)
                {
                    timeSpan = ex.RetryAfter;
                }
                catch (AggregateException ex) when (ex.InnerException is DocumentClientException de && de.StatusCode != null && (int)de.StatusCode == 429)
                {
                    timeSpan = de.RetryAfter;
                }

                await Task.Delay(timeSpan);
            }
        }
    }
}
