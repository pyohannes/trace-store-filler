using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System.Data;
using System.Runtime.InteropServices;

namespace TraceStoreFiller
{
    public class QueryExecutor
    {
        private ICslQueryProvider _provider;
        private KustoConnectionStringBuilder? _connectionStringBuilder;
        private ClientRequestProperties _requestProperties;

        public QueryExecutor(string clusterUri)
        {
            _connectionStringBuilder = new KustoConnectionStringBuilder($"{clusterUri}")
                .WithAadAzCliAuthentication();

            _provider = KustoClientFactory.CreateCslQueryProvider(_connectionStringBuilder);

            _requestProperties = new ClientRequestProperties();

            _requestProperties.SetOption(ClientRequestProperties.OptionQueryConsistency, ClientRequestProperties.OptionQueryConsistency_Weak);
        }

        public async Task<IDataReader> ExecuteQueryAsync(string query, string database = "CorrelationPlatformDB")
        {
            try
            {
                return await _provider.ExecuteQueryAsync(database, query, _requestProperties);
            } catch (Exception e)
            {
                Console.WriteLine($"Error: {e.Message}");

                Thread.Sleep(TimeSpan.FromMinutes(1));

                _provider = KustoClientFactory.CreateCslQueryProvider(_connectionStringBuilder);
                return await _provider.ExecuteQueryAsync(database, query, _requestProperties);
            }
        }

        public IDataReader ExecuteQuery(string query, string database = "CorrelationPlatformDB")
        {
            try
            {
                return _provider.ExecuteQuery(database, query, _requestProperties);
            } catch (Exception e)
            {
                Console.WriteLine($"Error: {e.Message}");

                Thread.Sleep(TimeSpan.FromMinutes(1));

                _provider = KustoClientFactory.CreateCslQueryProvider(_connectionStringBuilder);
                return _provider.ExecuteQuery(database, query, _requestProperties);
            }
        }

        public static string ToQueryDateTime(DateTime time)
        {
            return $"make_datetime({time.Year}, {time.Month}, {time.Day}, {time.Hour}, {time.Minute}, {time.Second})";
        }
    }
}
