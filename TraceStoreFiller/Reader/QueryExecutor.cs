using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System.Data;
using System.Runtime.InteropServices;

namespace TraceStoreFiller
{
    internal class QueryExecutor
    {
        private ICslQueryProvider _provider;
        private KustoConnectionStringBuilder? _connectionStringBuilder;

        public QueryExecutor(string clusterUri)
        {
            _connectionStringBuilder = new KustoConnectionStringBuilder($"{clusterUri}")
                .WithAadAzCliAuthentication();

            _provider = KustoClientFactory.CreateCslQueryProvider(_connectionStringBuilder);
        }

        public async Task<IDataReader> ExecuteQuery(string query, string database = "CorrelationPlatformDB")
        {
            try
            {
                return await _provider.ExecuteQueryAsync(database, query, new ClientRequestProperties());
            } catch (Exception e)
            {
                Console.WriteLine($"Error: {e.Message}");

                _provider = KustoClientFactory.CreateCslQueryProvider(_connectionStringBuilder);
                return await _provider.ExecuteQueryAsync(database, query, new ClientRequestProperties());
            }
        }

        public static string ToQueryDateTime(DateTime time)
        {
            return $"make_datetime({time.Year}, {time.Month}, {time.Day}, {time.Hour}, {time.Minute}, {time.Second})";
        }
    }
}
