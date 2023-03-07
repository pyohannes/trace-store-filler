using Azure.Storage.Blobs;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System.Data;
using System.Runtime.InteropServices;

namespace TraceStoreFiller
{
    public class BlobReader
    {
        private BlobServiceClient _blobServiceClient;
        private BlobContainerClient _containerClient;

        public BlobReader(string connectionString)
        {
            _blobServiceClient = new BlobServiceClient(connectionString);

            _containerClient = _blobServiceClient.GetBlobContainerClient("lakev1");
        }

        public BinaryData Read(string path)
        {
            var blobClient = _containerClient.GetBlobClient(path);

            var response = blobClient.DownloadContent();

            return response.Value.Content;
        }
    }
}
