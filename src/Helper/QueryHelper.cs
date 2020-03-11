using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace Hangfire.Azure.Helper
{
    internal static class QueryHelper
    {
        internal static IEnumerable<T> ToQueryResult<T>(this IDocumentQuery<T> query)
        {
            while (query.HasMoreResults)
            {
                Task<FeedResponse<T>> task = Task.Run(async () => await query.ExecuteNextWithRetriesAsync());
                task.Wait();
                foreach (T item in task.Result)
                {
                    yield return item;
                }
            }
        }

        internal static IEnumerable<T> ToQueryResult<T>(this IQueryable<T> queryable)
        {
            IDocumentQuery<T> query = queryable.AsDocumentQuery();
            return query.ToQueryResult();
        }


        /// <summary>
        /// Execute the function with retries on throttle
        /// </summary>
        internal static async Task<FeedResponse<T>> ExecuteNextWithRetriesAsync<T>(this IDocumentQuery<T> query)
        {
            while (true)
            {
                TimeSpan timeSpan;

                try
                {
                    return await query.ExecuteNextAsync<T>();
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