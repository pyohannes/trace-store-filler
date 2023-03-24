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
        private List<string> _prefixes;

        public ExportedBlobReader(string connectionString, string[] prefixes)
        {
            _blobServiceClient = new BlobServiceClient(connectionString);

            _containerClient = _blobServiceClient.GetBlobContainerClient("kustocopy3");
            _prefixes = new List<string>(prefixes);
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

            var blobName = _blobNames.Find(bn => bn.StartsWith($"{_prefixes[0]}/export_{_counter}_"));

            if (blobName == null && _prefixes.Count != 0)
            {
                _prefixes.RemoveAt(0);
                _counter = 1;
                return await GetNextBlobStream();
            }

            if (blobName == null)
            {
                var text = $"Blobs exhausted, read {_counter} blobs";
                Console.WriteLine(text);
                throw new ArgumentException(text);
            }

            _counter += 1;

            var blobClient = _containerClient.GetBlobClient(blobName);

            var result = await blobClient.DownloadContentAsync();

            Console.WriteLine($"Downloaded content for blob {blobName}");

            return result.Value.Content;
        }
    }
}
