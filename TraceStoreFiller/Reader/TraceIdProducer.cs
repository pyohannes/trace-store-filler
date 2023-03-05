using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System.Data;
using System.Runtime.InteropServices;

namespace TraceStoreFiller
{
    internal class TraceIdProducer
    {
        private QueryExecutor _queryExecutor;
        private DateTime _windowPosition;
        private DateTime _windowStart;
        private DateTime _windowEnd;
        private List<(string endpoint, string namespace_)>? _namespaces;
        private List<string> _cachedTraceIds = new();

        public TraceIdProducer(string clusterUri, DateTime windowStart, DateTime windowEnd)
        {
            _queryExecutor = new QueryExecutor(clusterUri);

            _windowStart = _windowPosition = windowStart;
            _windowEnd = windowEnd;
        }

        public async Task<List<string>> GetTraceIdsAsync(int count)
        {
            while (true)
            {
                try
                {
                    if (_namespaces == null)
                    {
                        _namespaces = await GetNamespaces(_windowStart, _windowEnd);
                    }

                    while (count > _cachedTraceIds.Count)
                    {
                        await FillCache();
                    }

                    var ids = _cachedTraceIds.GetRange(0, count);
                    _cachedTraceIds.RemoveRange(0, count);

                    return ids;
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Exception caught: {e.Message}, {e.StackTrace}");
                }
            }
        }

        private async Task FillCache()
        {
            if (_windowPosition == _windowEnd)
            {
                _windowPosition = _windowStart;
                _namespaces.RemoveAt(0);

                if (_namespaces.Count == 0)
                {
                    throw new IndexOutOfRangeException("All trace ids processed");
                }
            }

            var ns = _namespaces[0];

            Console.WriteLine($"Producing traces ids for namespace {ns}");

            var query =
                "TraceSpan " +
                $"| where startTime > {QueryExecutor.ToQueryDateTime(_windowPosition)} " +
                $"| where startTime <= {QueryExecutor.ToQueryDateTime(_windowPosition.AddSeconds(1))} " +
                $"| where endpoint == \"{ns.endpoint}\" " +
                $"| where namespace == \"{ns.namespace_}\" " +
                "| summarize count() by w3cTraceId " +
                "| sort by count_ desc";

            using (var reader = await _queryExecutor.ExecuteQuery(query))
            {
                while (reader.Read())
                {
                    _cachedTraceIds.Add(reader.GetString(0));
                }
            }

            Console.WriteLine($"Read {_cachedTraceIds.Count} trace ids for namespace {ns.namespace_} for timestamp {_windowPosition.ToString()}");

            _windowPosition += TimeSpan.FromSeconds(1);
        }

        private async Task<List<(string endpoint, string namespace_)>> GetNamespaces(DateTime windowStart, DateTime windowEnd)
        {
            var namespaces = new List<(string endpoint, string namespace_)>();

            var query =
                "TraceSpan " +
                $"| where startTime > {QueryExecutor.ToQueryDateTime(windowStart)} " +
                $"| where startTime <= {QueryExecutor.ToQueryDateTime(windowEnd)} " +
                "| summarize count() by endpoint, namespace" +
                "| sort by count_ desc";

            using (var reader = await _queryExecutor.ExecuteQuery(query))
            {
                while (reader.Read())
                {
                    namespaces.Add((reader.GetString(0), reader.GetString(1)));
                }
            }

            return namespaces;
        }
    }
}
