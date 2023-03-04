using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System.Data;
using System.Runtime.InteropServices;

namespace TraceStoreFiller
{
    internal class KustoReader
    {
        private ICslQueryProvider _provider;
        private DateTime _windowStart;
        private DateTime _windowEnd;
        private KustoConnectionStringBuilder? _connectionStringBuilder;

        public KustoReader(string clusterUri, DateTime windowsStart, DateTime windowEnd)
        {
            _connectionStringBuilder = new KustoConnectionStringBuilder($"{clusterUri}/CorrelationPlatformDB")
                .WithAadUserPromptAuthentication();

            _windowStart = windowsStart;
            _windowEnd = windowEnd;
        }

        public List<(string endpoint, string namespace_)> GetNamespaces()
        {
            var namespaces = new List<(string endpoint, string namespace_)>();

            var query =
                "TraceSpan " +
                $"| where startTime > {ToQueryDateTime(_windowStart)} " +
                $"| where startTime <= {ToQueryDateTime(_windowEnd)} " +
                "| summarize count() by endpoint, namespace" +
                "| sort by count_ asc";

            using (var reader = ExecuteQuery(query))
            {
                while (reader.Read())
                {
                    namespaces.Add((reader.GetString(0), reader.GetString(1)));
                }
            }

            return namespaces;
        }

        public List<string> GetTraceIdsForNamespace(string endpoint, string namespace_)
        {
            var traceIds = new List<string>();

            var query =
                "TraceSpan " +
                $"| where startTime > {ToQueryDateTime(_windowStart)} " +
                $"| where startTime <= {ToQueryDateTime(_windowEnd)} " +
                $"| where endpoint == \"{endpoint}\" " +
                $"| where namespace == \"{namespace_}\" " +
                "| summarize count() by w3cTraceId " +
                "| sort by count_ asc";

            using (var reader = ExecuteQuery(query))
            {
                while (reader.Read())
                {
                    traceIds.Add(reader.GetString(0));
                }
            }

            return traceIds;
        }

        public List<List<string>> GetLinkedTraceIds(List<string> traceIds)
        {
            var linkedTraceIds = new HashSet<string>(traceIds);
            var traceIdSets = new List<List<string>>(traceIds.Select(id => new List<string>(new string[] { id })));

            var newTraceIds = new HashSet<string>(traceIds);

            while (true)
            {
                var idsStr = $"({string.Join(",", newTraceIds.Select(id => $"\"{id}\""))})";

                var query =
                    "SpanLink " +
                    $"| where srcW3CTraceId in {idsStr}" +
                    $"| where destW3CTraceId in {idsStr}" +
                    "| project srcW3CTraceId, destW3CTraceId";

                newTraceIds.Clear();

                using (var reader = ExecuteQuery(query))
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

            return traceIdSets;
        }

        public List<TraceSet> GetTraceSets(List<List<string>> traceSetIds)
        {
            var traceSets = new List<TraceSet>();

            foreach (var traceSetId in traceSetIds)
            {
                var idsStr = $"({string.Join(",", traceSetId.Select(id => $"\"{id}\""))})";

                var query =
                    "TraceSpan " +
                    $"| where w3cTraceId in {idsStr}";

                var spans = new List<Span>();

                using (var reader = ExecuteQuery(query))
                {
                    while (reader.Read())
                    {
                        spans.Add(Span.From(reader));
                    }
                }

                traceSets.Add(CreateTraceSetFromSpans(spans, traceSetId));
            }

            return traceSets;
        }

        private TraceSet CreateTraceSetFromSpans(List<Span> spans, List<string> traceSetIds)
        {
            var tc = new TraceSet();
            tc.TraceSetId = new Guid().ToString();

            tc.traces = GetTraceListFromSpans(spans, traceSetIds);

            foreach (var trace in tc.traces)
            {
                tc.startTime = tc.startTime < trace.startTime ? tc.startTime : trace.startTime;
                tc.endTime = tc.endTime > trace.endTime ? tc.endTime : trace.endTime;
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

                foreach (var span in trace.chunks.SelectMany(chunk => chunk.spans))
                {
                    trace.startTime = trace.startTime < span.StartTime ? trace.startTime : span.StartTime;
                    trace.endTime = trace.endTime > span.EndTime ? trace.endTime : span.EndTime;
                }

                traces.Add(trace);
            }

            return traces ;
        }

        public List<TraceChunk> GetTraceChunksFromSpans(IEnumerable<Span> spans)
        {
            var chunks = new List<TraceChunk>();

            var roles = new HashSet<(string? role, string? roleInstance)>(spans.ToList().Select(span =>
            {
                if (span.cloudIdentity != null)
                {
                    _ = span.cloudIdentity.TryGetValue("role", out var role);
                    _ = span.cloudIdentity.TryGetValue("roleInstance", out var roleInstance);

                    return ((string?)role, (string?)roleInstance);
                }

                return (null,  null);
            }));

            foreach (var role in roles)
            {
                var chunk = new TraceChunk();

                chunk.cloudRole = role.role;
                chunk.cloudRoleInstance = role.roleInstance;

                foreach (var span in spans)
                {
                    object? r = null, rInstance = null;

                    if (span.cloudIdentity != null)
                    {
                        _ = span.cloudIdentity.TryGetValue("role", out r);
                        _ = span.cloudIdentity.TryGetValue("roleInstance", out rInstance);
                    }

                    if (role.role == (string?)r && role.roleInstance == (string?)rInstance)
                    {
                        chunk.spans.Add(span);
                    }
                }

                chunks.Add(chunk);
            }

            return chunks;
        }

        private IDataReader ExecuteQuery(string query)
        {
            if (_provider == null)
            {
                _provider = KustoClientFactory.CreateCslQueryProvider(_connectionStringBuilder);
            }

            try
            {
                return _provider.ExecuteQuery(query);
            } catch (Exception e)
            {
                Console.WriteLine($"Error: {e.Message}");

                _provider = KustoClientFactory.CreateCslQueryProvider(_connectionStringBuilder);
                return _provider.ExecuteQuery(query);
            }
        }

        private string ToQueryDateTime(DateTime time)
        {
            return $"make_datetime({time.Year}, {time.Month}, {time.Day}, {time.Hour}, {time.Minute}, {time.Second})";
        }
    }
}
