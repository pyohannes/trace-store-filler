using Azure.Storage.Blobs;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using System.Data;
using System.Runtime.InteropServices;
using System.Text;

namespace TraceStoreFiller
{
    internal class KustoIndexWriter
    {
        private KustoConnectionStringBuilder _connectionStringBuilder;
        private string _dbName = "traceindexv1";
        private KustoIngester _kustoIngester;

        public KustoIndexWriter(string clusterUri)
        {
            _kustoIngester = new KustoIngester(clusterUri);
        }

        public async Task WriteIndices(List<TraceSetIndex> traceSetIndex, List<TraceIndex> traceIndex, List<TraceChunkIndex> traceChunkIndex)
        {
            /*StringBuilder query = new();
            foreach (var tsi in traceSetIndex)
            {
                query.AppendLine($"{ToQS(tsi.TraceSetId)},{ToQS(ToIngestDateTime(tsi.StartTime))},{ToQS(ToIngestDateTime(tsi.EndTime))},{(long)tsi.Duration.TotalMilliseconds}");
            }

            await _kustoIngester.ExecuteQuery(query.ToString(), _dbName, "TraceSetIndex");
            */

            StringBuilder query = new();
            foreach (var ti in traceIndex)
            {
                query.AppendLine($"{ToQS(ti.TraceId)},{ToQS(ToIngestDateTime(ti.StartTime))},{ToQS(ToIngestDateTime(ti.EndTime))},{(long)ti.Duration.TotalMilliseconds}");
            }

            await _kustoIngester.ExecuteQuery(query.ToString(), _dbName, "TraceIndex");

            Console.WriteLine($"Wrote TraceIndex, {traceIndex.Count} rows");

            query = new();
            foreach (var tci in traceChunkIndex)
            {
                query.AppendLine($"{ToQS(tci.TraceSetId)},{ToQS(tci.TraceId)},{ToQS(tci.cloudRole)},{ToQS(tci.cloudRoleInstance)},{ToQS(tci.Namespace)},{ToQS(tci.Endpoint)},{ToQS(tci.DataRegion)},{tci.Success.ToString()},{ToQS(tci.RootOperationName)},{ToQS(tci.BlobPath)},{tci.SpanCount}");
            }

            await _kustoIngester.ExecuteQuery(query.ToString(), _dbName, "TraceChunkIndex");

            Console.WriteLine($"Wrote TraceChunkIndex, {traceChunkIndex.Count} rows");
        }

        private string ToIngestDateTime(DateTime dateTime)
        {
            return dateTime.ToString();
        }

    private string ToQS(string? s)
    {
        if (s == null)
        {
            return "null";
        } else
        {
            return $"\"{s}\"";
        }
    }
    }

}
