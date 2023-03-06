using Azure.Storage.Blobs;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System.Data;
using System.Runtime.InteropServices;

namespace TraceStoreFiller
{
    internal class ExportedBlobReader
    {
        private BlobServiceClient _blobServiceClient;
        private BlobContainerClient _containerClient;

        private List<string> _blobNames = new();
        private int _counter = 1;

        public ExportedBlobReader(string connectionString)
        {
            _blobServiceClient = new BlobServiceClient(connectionString);

            _containerClient = _blobServiceClient.GetBlobContainerClient("kustocopy2");

        }

        private async Task Init() {
            await foreach (var blob in _containerClient.GetBlobsAsync())
            {
                _blobNames.Add(blob.Name);
            }
        }

        public async Task<BinaryData> GetNextBlobStream()
        {
            if (_blobNames.Count == 0)
            {
                await Init();
            }

            var blobName = _blobNames.Find(bn => bn.StartsWith($"export_{_counter}"));

            if (blobName == null)
            {
                throw new ArgumentException($"Blobs exhausted, read {_counter} blobs");
            }

            _counter += 1;

            var blobClient = _containerClient.GetBlobClient(blobName);

            var result = await blobClient.DownloadContentAsync();

            Console.WriteLine($"Downloaded content for blob {blobName}");

            return result.Value.Content;
        }
    }
}
