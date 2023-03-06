﻿using Azure.Storage.Blobs;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System.Data;
using System.Runtime.InteropServices;

namespace TraceStoreFiller
{
    internal class BlobStoreWriter
    {
        private BlobServiceClient _blobServiceClient;
        private BlobContainerClient _containerClient;

        public Func<TraceChunk, string, Task> WriteIndex;

        public BlobStoreWriter(string connectionString)
        {
            _blobServiceClient = new BlobServiceClient(connectionString);

            _containerClient = _blobServiceClient.GetBlobContainerClient("lakev1");
        }

        public async Task WriteBlob(List<TraceChunk> chunks, Stream dataStream, DateTime timeCategory)
        {
            var path = $"{timeCategory.Year}/{timeCategory.Month}/{timeCategory.Day}/{timeCategory.Hour}/{timeCategory.Minute}";

            BlobClient blobClient;
            string fileName;
            while (true)
            {
                fileName = $"{path}/{Guid.NewGuid().ToString()}.parquet";
                blobClient = _containerClient.GetBlobClient(fileName);

                if (!await blobClient.ExistsAsync())
                {
                    break;
                }
            }

            await blobClient.UploadAsync(dataStream, true);

            Console.WriteLine($"Uploaded blob to path {fileName}, containing {chunks.Count} chunks");

            foreach (var chunk in chunks)
            {
                await WriteIndex(chunk, fileName);
            }
        }
    }
}
