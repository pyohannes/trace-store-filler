using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using System.Data;
using System.Runtime.InteropServices;

namespace TraceStoreFiller
{
    internal class KustoIngester
    {
        private IKustoIngestClient _provider;
        private KustoConnectionStringBuilder? _connectionStringBuilder;

        public KustoIngester(string clusterUri)
        {
            _connectionStringBuilder = new KustoConnectionStringBuilder($"{clusterUri}")
                .WithAadAzCliAuthentication();

            _provider = KustoIngestFactory.CreateQueuedIngestClient(_connectionStringBuilder);
        }

        public async Task<IKustoIngestionResult> ExecuteQuery(string csvQuery, string database, string table)
        {
            var kustoIngestionProperties = new KustoIngestionProperties(databaseName: database, tableName: table);
            kustoIngestionProperties.Format = DataSourceFormat.csv;

            var stream = new MemoryStream();

            var writer = new StreamWriter(stream);
            writer.Write(csvQuery);
            writer.Flush();

            stream.Seek(0, SeekOrigin.Begin);

            try
            {
                return await _provider.IngestFromStreamAsync(stream, kustoIngestionProperties);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error: {e.Message}");

                stream.Seek(0, SeekOrigin.Begin);
                _provider = KustoIngestFactory.CreateDirectIngestClient(_connectionStringBuilder);
                return await _provider.IngestFromStreamAsync(stream, kustoIngestionProperties);
            }
        }
    }
}
