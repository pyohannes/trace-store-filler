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
        public string Endpoint { get; }
        public string Namespace { get; }
        private string? _lastPath;
        private int _directoryCounter = 0;
        private BlobServiceClient _blobServiceClient;
        private BlobContainerClient _containerClient;

        public Func<TraceChunk, string, Task> WriteIndex;

        public BlobStoreWriter(string connectionString, string endpoint, string namespace_)
        {
            Endpoint = endpoint;
            Namespace = namespace_;

            _blobServiceClient = new BlobServiceClient(connectionString);

            _containerClient = _blobServiceClient.GetBlobContainerClient("lakev1");
        }

        public async Task WriteBlob(List<TraceChunk> chunks, Stream dataStream, DateTime timeCategory)
        {
            var path = $"{timeCategory.Year}/{timeCategory.Month}/{timeCategory.Day}/{timeCategory.Hour}/{timeCategory.Minute}/{Endpoint}/{Namespace}";

            if (path != _lastPath)
            {
                _directoryCounter = 0;
            }

            var fileName = $"{path}/{_directoryCounter}.parquet";
            BlobClient blobClient = _containerClient.GetBlobClient(fileName);

            await blobClient.UploadAsync(dataStream, true);

            foreach (var chunk in chunks)
            {
                await WriteIndex(chunk, fileName);
            }

            _directoryCounter += 1;
            _lastPath = path;
        }
    }
}
