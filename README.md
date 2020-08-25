# Hangfire.AzureDocumentDB

[![Official Site](https://img.shields.io/badge/site-hangfire.io-blue.svg)](http://hangfire.io)
[![Latest version](https://img.shields.io/nuget/v/Hangfire.AzureDocumentDB.svg)](https://www.nuget.org/packages/Hangfire.AzureDocumentDB)
[![Downloads](https://img.shields.io/nuget/dt/Hangfire.AzureDocumentDB.svg)](https://www.nuget.org/packages/Hangfire.AzureDocumentDB)
[![Build status](https://ci.appveyor.com/api/projects/status/uvxh94dhxcokga47?svg=true)](https://ci.appveyor.com/project/imranmomin/hangfire-azuredocumentdb)

This repo will add a [Microsoft Azure Document DB](https://www.nuget.org/packages/Microsoft.Azure.DocumentDB/) storage support to [Hangfire](http://hangfire.io) - fire-and-forget, delayed and recurring tasks runner for .NET. Scalable and reliable background job runner. Supports multiple servers, CPU and I/O intensive, long-running and short-running jobs.


## Installation

[Hangfire.AzureDocumentDB](https://www.nuget.org/packages/Hangfire.AzureDocumentDB) is available on NuGet.


Package Manager
```powershell
PM> Install-Package Hangfire.AzureDocumentDB
```

.NET CLI
```
> dotnet add package Hangfire.AzureDocumentDB
```

PackageReference
```xml
<PackageReference Include="Hangfire.AzureDocumentDB" Version="0.0.0" />
```

## Usage

Use one the following ways to initialize `DocumentDbStorage`

```csharp
GlobalConfiguration.Configuration.UseAzureDocumentDbStorage("<url>", "<authSecret>", "<databaseName>", "<collectionName>");

Hangfire.Azure.DocumentDbStorage storage = new Hangfire.Azure.DocumentDbStorage("<url>", "<authSecret>", "<databaseName>", "<collectionName>");
GlobalConfiguration.Configuration.UseStorage(storage);
```

```csharp
// customize any options
Hangfire.Azure.DocumentDbStorageOptions options = new Hangfire.Azure.DocumentDbStorageOptions
{
    RequestTimeout = TimeSpan.FromSeconds(30),
    ExpirationCheckInterval = TimeSpan.FromMinutes(2),
    CountersAggregateInterval = TimeSpan.FromMinutes(2),
    QueuePollInterval = TimeSpan.FromSeconds(15),
    ConnectionMode = ConnectionMode.Direct,
    ConnectionProtocol = Protocol.Tcp,
    EnablePartition = false // default: false true; to enable partition on /type
};

GlobalConfiguration.Configuration.UseAzureDocumentDbStorage("<url>", "<authSecret>", "<databaseName>", "<collectionName>", options);

// or 

Hangfire.Azure.DocumentDbStorage storage = new Hangfire.Azure.DocumentDbStorage("<url>", "<authSecret>", "<databaseName>", "<collectionName>", options);
GlobalConfiguration.Configuration.UseStorage(storage);
```

## Recommendations
- Keep seperate database/collection for the hangfire. (Now you can [enable free tier](https://docs.microsoft.com/en-us/azure/cosmos-db/optimize-dev-test#azure-cosmos-db-free-tier) on Azure)
- Enable partitioning by ```/type```

## SDK Support
This package only support using [Microsoft.Azure.DocumentDB](https://www.nuget.org/packages/Microsoft.Azure.DocumentDB/). If you want the support for the latest SDK v3 [Microsoft.Azure.Cosmos](https://www.nuget.org/packages/Microsoft.Azure.Cosmos), you will have to use [Hangfire.AzureCosmosDb](https://github.com/imranmomin/Hangfire.AzureCosmosDb)

## Questions? Problems?

Open-source project are developing more smoothly, when all discussions are held in public.

If you have any questions or problems related to Hangfire.AzureDocumentDB itself or this storage implementation or want to discuss new features, please create under [issues](https://github.com/imranmomin/Hangfire.AzureDocumentDB/issues/new) and assign the correct label for discussion. 

If you've discovered a bug, please report it to the [GitHub Issues](https://github.com/imranmomin/Hangfire.AzureDocumentDB/pulls). Detailed reports with stack traces, actual and expected behavours are welcome.
