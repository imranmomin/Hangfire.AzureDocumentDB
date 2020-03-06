﻿using System;

using Microsoft.Azure.Documents;

using Newtonsoft.Json;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Documents
{
    internal class Queue : DocumentBase
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("job_id")]
        public string JobId { get; set; }

        [JsonProperty("created_on")]
        [JsonConverter(typeof(UnixDateTimeConverter))]
        public DateTime? CreatedOn { get; set; }

        [JsonProperty("fetched_at")]
        [JsonConverter(typeof(UnixDateTimeConverter))]
        public DateTime? FetchedAt { get; set; }

        public override DocumentTypes DocumentType => DocumentTypes.Queue;
    }
}
