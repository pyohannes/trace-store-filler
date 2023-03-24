using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System.Data;
using System.Runtime.InteropServices;

namespace TraceStoreFiller
{
    internal class ParquetWriterFactory
    {
        private string _blobStorageConnectionString;

        public ParquetWriterFactory(string blobStorageConnectionString)
        {
            _blobStorageConnectionString = blobStorageConnectionString;
        }

        public ParquetWriter GetWriter(IndexProducer producer, string endpoint, string namespace_)
        {
            var blobStoreWriter = new BlobStoreWriter(_blobStorageConnectionString);

            var parquetWriter = new ParquetWriter(endpoint, namespace_);

            parquetWriter.WriteStream = blobStoreWriter.WriteBlob;

            return parquetWriter;
        }
    }
}
