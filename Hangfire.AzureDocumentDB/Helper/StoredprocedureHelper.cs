using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Hangfire.Azure.Documents;

using Microsoft.Azure.Documents.Client;

namespace Hangfire.Azure.Helper
{
    internal static class StoredprocedureHelper
    {
        private static Uri spPersistDocumentsUri;
        private static Uri spExpireDocumentsUri;
        private static Uri spDeleteDocumentsUri;
        private static Uri spUpsertDocumentsUri;

        internal static void Setup(string databaseName, string collectionName)
        {
            spPersistDocumentsUri = UriFactory.CreateStoredProcedureUri(databaseName, collectionName, "persistDocuments");
            spExpireDocumentsUri = UriFactory.CreateStoredProcedureUri(databaseName, collectionName, "expireDocuments");
            spDeleteDocumentsUri = UriFactory.CreateStoredProcedureUri(databaseName, collectionName, "deleteDocuments");
            spUpsertDocumentsUri = UriFactory.CreateStoredProcedureUri(databaseName, collectionName, "upsertDocuments");
        }

        internal static int ExecuteUpsertDocuments<T>(this DocumentClient client, Data<T> data, RequestOptions options = null, CancellationToken cancellationToken = default)
        {
            int affected = 0;
            Data<T> records = new Data<T>(data.Items);
            do
            {
                records.Items = data.Items.Skip(affected).ToList();
                Task<StoredProcedureResponse<int>> task = client.ExecuteStoredProcedureWithRetriesAsync<int>(spUpsertDocumentsUri, options, cancellationToken, records);
                task.Wait();
                affected += task.Result;
            } while (affected < data.Items.Count);
            return affected;
        }

        internal static int ExecuteDeleteDocuments(this DocumentClient client, string query, RequestOptions options = null, CancellationToken cancellationToken = default)
        {
            int affected = 0;
            ProcedureResponse response;
            do
            {
                Task<StoredProcedureResponse<ProcedureResponse>> task = client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spDeleteDocumentsUri, options, cancellationToken, query);
                task.Wait();
                response = task.Result;
                affected += response.Affected;
            } while (response.Continuation);
            return affected;
        }

        internal static void ExecutePersistDocuments(this DocumentClient client, string query, RequestOptions options = null, CancellationToken cancellationToken = default)
        {
            ProcedureResponse response;
            do
            {
                Task<StoredProcedureResponse<ProcedureResponse>> task = client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spPersistDocumentsUri, options, cancellationToken, query);
                task.Wait();
                response = task.Result;
            } while (response.Continuation);
        }

        internal static void ExecuteExpireDocuments(this DocumentClient client, string query, int epoch, RequestOptions options = null, CancellationToken cancellationToken = default)
        {
            ProcedureResponse response;
            do
            {
                Task<StoredProcedureResponse<ProcedureResponse>> task = client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spExpireDocumentsUri, options, cancellationToken, query, epoch);
                task.Wait();
                response = task.Result;
            } while (response.Continuation);
        }
    }
}
