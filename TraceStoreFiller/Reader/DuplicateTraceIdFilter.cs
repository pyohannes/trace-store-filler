using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System.Data;
using System.Runtime.InteropServices;
using System.Threading.Channels;

namespace TraceStoreFiller
{
    internal class DuplicateTraceIdFilter
    {
        private List<string> _seenTraceIds = new();
        private List<string> _cachedTraceIds = new();
        private int _batchSize;

        private QueryExecutor _kustoIndexQuery;

        public ChannelReader<TraceSet> UnfilteredTraceSets;
        public ChannelWriter<TraceSet> FilteredTraceSets;

        public DuplicateTraceIdFilter(string kustoIndexConnectionString, int batchSize = 200)
        {
            _batchSize = batchSize;

            _kustoIndexQuery = new QueryExecutor(kustoIndexConnectionString);
        }

        public async Task StartProcessingAsync()
        {
            while (true)
            {
                var traceSets = new List<TraceSet>();

                while (traceSets.Count < _batchSize)
                {
                    traceSets.Add(await UnfilteredTraceSets.ReadAsync());
                }

                var filteredTraceSets = new Dictionary<string, TraceSet>();

                foreach (var traceSet in traceSets)
                {
                    var traceId = traceSet.traces[0].TraceId;
                    if (!_seenTraceIds.Contains(traceId))
                    {
                        _seenTraceIds.Add(traceId);
                        filteredTraceSets[traceId] = traceSet;
                    }
                }

                if (filteredTraceSets.Count == 0)
                {
                    continue;
                }

                var query = $"TraceIndex \n" +
                            $"| where TraceId in ({string.Join(",", filteredTraceSets.Keys.Select(id => $"\"{id}\""))}) \n" +
                            $"| project TraceId";
                using (var reader = await _kustoIndexQuery.ExecuteQueryAsync(query, database: "traceindexv1"))
                {
                    while (reader.Read())
                    {
                        var traceid = reader.GetString(0);

                        if (filteredTraceSets.ContainsKey(traceid))
                        {
                            filteredTraceSets.Remove(traceid);
                        }
                    }
                }

                foreach (var t in filteredTraceSets.Values)
                {
                    await FilteredTraceSets.WriteAsync(t);
                }

                if (_seenTraceIds.Count > 2000000)
                {
                    _seenTraceIds.RemoveRange(0, 1000000);
                }
            }

        }
    }
}
