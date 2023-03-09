using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System.Data;
using System.Runtime.InteropServices;
using System.Threading.Channels;

namespace TraceStoreFiller
{
    internal class TraceSetIdsProducer
    {
        private QueryExecutor _queryExecutor;
        public Func<int, Task<List<string>>> TraceIdProducer;
        private int _batchSize;
        private List<List<string>> _cachedTraceSetIds = new();

        public ChannelWriter<List<string>> TraceSetIdWriter;

        public TraceSetIdsProducer(string clusterUri, int batchSize = 200)
        {
            _queryExecutor = new QueryExecutor(clusterUri);
            _batchSize = batchSize;
        }

        public async Task StartProcessingAsync()
        {
            while (true)
            {
                try
                {
                    var traceIdBatch = await TraceIdProducer(_batchSize);

                    var linkedTraceIds = new HashSet<string>(traceIdBatch);
                    var traceIdSets = new List<List<string>>(traceIdBatch.Select(id => new List<string>(new string[] { id })));

                    var newTraceIds = new HashSet<string>(traceIdBatch);

                    while (true)
                    {
                        var idsStr = $"({string.Join(",", newTraceIds.Select(id => $"\"{id}\""))})";

                        var query =
                            "SpanLink " +
                            $"| where srcW3CTraceId in {idsStr}" +
                            $"| where destW3CTraceId in {idsStr}" +
                            "| project srcW3CTraceId, destW3CTraceId";

                        newTraceIds.Clear();

                        using (var reader = await _queryExecutor.ExecuteQueryAsync(query))
                        {
                            while (reader.Read())
                            {
                                newTraceIds.Add(reader.GetString(0));
                                newTraceIds.Add(reader.GetString(1));

                                // merge linked sets
                                var set1 = traceIdSets.Find(ids => ids.Contains(reader.GetString(0)));
                                var set2 = traceIdSets.Find(ids => ids.Contains(reader.GetString(1)));

                                if (set1 == null && set2 == null)
                                {
                                    traceIdSets.Add(new List<string>(new string[] { reader.GetString(0) }));
                                    traceIdSets.Add(new List<string>(new string[] { reader.GetString(1) }));
                                }
                                else if (set1 == null)
                                {
                                    set2.Add(reader.GetString(0));
                                }
                                else if (set2 == null)
                                {
                                    set1.Add(reader.GetString(1));
                                }
                                else
                                {
                                    traceIdSets.Remove(set1);
                                    traceIdSets.Remove(set2);

                                    set1.AddRange(set2);
                                    traceIdSets.Add(set1);
                                }
                            }
                        }

                        if ((newTraceIds.Count == 0) || linkedTraceIds.Intersect(newTraceIds).ToList().Count == 0)
                        {
                            break;
                        }

                        foreach (var traceId in newTraceIds)
                        {
                            linkedTraceIds.Add(traceId);
                        }
                    }

                    foreach (var traceSetIds in traceIdSets)
                    {
                        await TraceSetIdWriter.WriteAsync(traceSetIds);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Exception caught: {e.Message}, {e.StackTrace}");
                }
            }
        }
    }
}
