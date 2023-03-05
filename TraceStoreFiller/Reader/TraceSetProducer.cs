using System.Data;
using System.Threading.Channels;

namespace TraceStoreFiller
{
    internal class TraceSetProducer
    {
        private QueryExecutor _queryExecutor;
        private int _batchSize;
        private List<TraceSet> _cachedTraceSets = new();

        private const int _traceSetSizeLimit = 10000;

        public ChannelReader<List<string>> TraceSetIdReader;

        public ChannelWriter<TraceSet> TraceSetWriter;

        public TraceSetProducer(string clusterUri, int batchSize = 2)
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
                    var traceSetId = await TraceSetIdReader.ReadAsync();

                    var idsStr = $"({string.Join(",", traceSetId.Select(id => $"\"{id}\""))})";

                    var query =
                        "SpanLink " +
                        $"| where srcW3CTraceId in {idsStr}" +
                        $"| where destW3CTraceId in {idsStr}" +
                        "| project srcW3CTraceId, srcW3CSpanId, destW3CTraceId, destW3CSpanId";

                    var links = new Dictionary<(string, string), List<(string, string)>>();

                    using (var reader = await _queryExecutor.ExecuteQuery(query))
                    {
                        while (reader.Read())
                        {
                            var key = (reader.GetString(0), reader.GetString(1));
                            var value = (reader.GetString(2), reader.GetString(3));

                            if (!links.ContainsKey(key))
                            {
                                links[key] = new List<(string, string)>();
                            }

                            links[key].Add(value);
                        }
                    }

                    query =
                        "TraceSpan \n" +
                        $"| where w3cTraceId in {idsStr} \n" +
                        $"| count";

                    using (var reader = await _queryExecutor.ExecuteQuery(query))
                    {
                        if (reader.Read())
                        {
                            var c = reader.GetInt64(0);

                            if (c > _traceSetSizeLimit)
                            {
                                Console.WriteLine($"Skipping trace set of size {c}");
                                continue;
                            }
                        }
                    }

                    query =
                        "TraceSpan " +
                        $"| where w3cTraceId in {idsStr}";

                    var spans = new List<Span>();

                    using (var reader = await _queryExecutor.ExecuteQuery(query))
                    {
                        while (reader.Read())
                        {
                            var span = Span.From(reader);

                            if (links.ContainsKey((span.TraceId, span.SpanId)))
                            {
                                span.links = links[(span.TraceId, span.SpanId)];
                            }
                            spans.Add(span);
                        }
                    }

                    Console.WriteLine($"Read trace set of size {spans.Count}");

                    await TraceSetWriter.WriteAsync(CreateTraceSetFromSpans(spans, traceSetId));
                }
                catch (Exception e)
                {
                    Thread.Sleep(TimeSpan.FromMinutes(1));

                    Console.WriteLine($"Exception caught, skipping trace set: {e.Message}");
                    continue;
                }
            }
        }

        private TraceSet CreateTraceSetFromSpans(List<Span> spans, List<string> traceSetIds)
        {
            var tc = new TraceSet();
            tc.TraceSetId = Guid.NewGuid().ToString();

            tc.traces = GetTraceListFromSpans(spans, traceSetIds);

            tc.startTime = tc.traces[0].startTime;
            tc.endTime = tc.traces[0].endTime;

            foreach (var trace in tc.traces)
            {
                tc.startTime = tc.startTime > trace.startTime ? trace.startTime : tc.startTime;
                tc.endTime = tc.endTime > trace.endTime ? tc.endTime : trace.endTime;
                trace.traceSet = tc;
            }

            return tc;
        }

        public List<Trace> GetTraceListFromSpans(IEnumerable<Span> spans, List<string> traceIds)
        {
            var traces = new List<Trace>();

            foreach (var traceId in traceIds)
            {
                var trace = new Trace();
                trace.TraceId = traceId;

                trace.chunks = GetTraceChunksFromSpans(spans.ToList().FindAll(span => span.TraceId == traceId));

                trace.startTime = trace.chunks[0].spans[0].StartTime;
                trace.endTime = trace.chunks[0].spans[0].EndTime;

                foreach (var chunk in trace.chunks)
                {
                    chunk.trace = trace;
                }

                foreach (var span in trace.chunks.SelectMany(chunk => chunk.spans))
                {
                    trace.startTime = trace.startTime > span.StartTime ? span.StartTime : trace.startTime;

                    trace.endTime = trace.endTime > span.EndTime ? trace.endTime : span.EndTime;
                }

                traces.Add(trace);
            }

            return traces;
        }

        public List<TraceChunk> GetTraceChunksFromSpans(IEnumerable<Span> spans)
        {
            var chunks = new List<TraceChunk>();

            var roles = new HashSet<(string? role, string? roleInstance, string namespace_, string endpoint, string region)>(spans.ToList().Select(span =>
            {
                if (span.cloudIdentity != null)
                {
                    _ = span.cloudIdentity.TryGetValue("role", out var role);
                    _ = span.cloudIdentity.TryGetValue("roleInstance", out var roleInstance);

                    return ((string?)role, (string?)roleInstance, span.Namespace, span.Endpoint, span.DataRegion);
                }

                return (null, null, span.Namespace, span.Endpoint, span.DataRegion);
            }));

            foreach (var role in roles)
            {
                var chunk = new TraceChunk();

                chunk.cloudRole = role.role;
                chunk.cloudRoleInstance = role.roleInstance;
                chunk.Endpoint = role.endpoint;
                chunk.Namespace = role.namespace_;
                chunk.DataRegion = role.region;
                chunk.Success = true;

                foreach (var span in spans)
                {
                    object? r = null, rInstance = null;

                    if (span.cloudIdentity != null)
                    {
                        _ = span.cloudIdentity.TryGetValue("role", out r);
                        _ = span.cloudIdentity.TryGetValue("roleInstance", out rInstance);
                    }

                    if (role.role == (string?)r
                        && role.roleInstance == (string?)rInstance
                        && role.namespace_ == span.Namespace
                        && role.endpoint == span.Endpoint
                        && role.region == span.DataRegion)
                    {
                        chunk.spans.Add(span);

                        if (span.Success == false)
                        {
                            chunk.Success = false;
                        }
                    }
                }

                chunk.spans.Sort((s1, s2) => (s1.StartTime < s2.StartTime) ? -1 : 1);
                chunk.RootOperationName = chunk.spans[0].Name;

                chunks.Add(chunk);
            }

            return chunks;
        }

    }
}
