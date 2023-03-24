using Azure.Storage.Blobs;
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

        public BlobStoreWriter(string connectionString)
        {
            _blobServiceClient = new BlobServiceClient(connectionString);

            _containerClient = _blobServiceClient.GetBlobContainerClient("lakenestedv1");
        }

        public async Task WriteBlob(List<Trace> chunks, Stream dataStream, DateTime timeCategory, string endpoint, string namespace_)
        {
            var path = $"{timeCategory.Year}/{timeCategory.Month}/{timeCategory.Day}/{timeCategory.Hour}/{timeCategory.Minute}";

            BlobClient blobClient;
            string fileName;
            while (true)
            {
                fileName = $"{path}/{endpoint}-{namespace_}-{Guid.NewGuid().ToString()}.parquet";
                blobClient = _containerClient.GetBlobClient(fileName);

                if (!await blobClient.ExistsAsync())
                {
                    break;
                }
            }

            await blobClient.UploadAsync(dataStream, true);

            Console.WriteLine($"Uploaded blob to path {fileName}, containing {chunks.Count} traces");
        }
    }
}
