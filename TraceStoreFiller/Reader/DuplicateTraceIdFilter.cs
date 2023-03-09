using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System.Data;
using System.Runtime.InteropServices;

namespace TraceStoreFiller
{
    internal class DuplicateTraceIdFilter
    {
        public Func<int, Task<List<string>>> TraceIdProducer;
        private List<string> _seenTraceIds = new();

        private List<string> _cachedTraceIds = new();
        private int _batchSize;

        private QueryExecutor _kustoIndexQuery;

        public DuplicateTraceIdFilter(string kustoIndexConnectionString, int batchSize = 200)
        {
            _batchSize = batchSize;

            _kustoIndexQuery = new QueryExecutor(kustoIndexConnectionString);
        }

        public async Task<List<string>> GetTraceIdsAsync(int count)
        {
            while (count > _cachedTraceIds.Count)
            {
                try
                {
                    await FillCache();
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Exception caught: {e.Message}, {e.StackTrace}");
                }
            }

            var ids = _cachedTraceIds.GetRange(0, count);
            _cachedTraceIds.RemoveRange(0, count);

            return ids;
        }

        public async Task FillCache()
        {
            var traceIdBatch = await TraceIdProducer(_batchSize);

            foreach (var traceId in traceIdBatch)
            {
                if (!_seenTraceIds.Contains(traceId))
                {
                    _seenTraceIds.Add(traceId);
                    _cachedTraceIds.Add(traceId);
                }
            }

            var query = $"TraceIndex \n" +
                        $"| where TraceId in ({string.Join(",", traceIdBatch.Select(id => $"\"{id}\""))}) \n" +
                        $"| project TraceId";
            using (var reader = await _kustoIndexQuery.ExecuteQueryAsync(query, database:  "traceindexv1"))
            {
                while (reader.Read())
                {
                    var traceid = reader.GetString(0);

                    if (_cachedTraceIds.Contains(traceid))
                    {
                        _cachedTraceIds.Remove(traceid);
                    }
                }
            }

            if (_seenTraceIds.Count > 2000000)
            {
                _seenTraceIds.RemoveRange(0, 1000000);
            }

        }
    }
}
