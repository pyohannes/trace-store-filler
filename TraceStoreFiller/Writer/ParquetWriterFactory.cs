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

        public ParquetWriter GetWriter(string endpoint, string namespace_, IndexProducer producer)
        {
            var blobStoreWriter = new BlobStoreWriter(_blobStorageConnectionString, endpoint, namespace_);
            blobStoreWriter.WriteIndex = producer.IndexTraceChunk;

            var parquetWriter = new ParquetWriter(endpoint, namespace_);

            parquetWriter.WriteStream = blobStoreWriter.WriteBlob;

            return parquetWriter;
        }
    }
}
