using System.Data;
using System.Threading.Channels;

namespace TraceStoreFiller
{
    internal class TraceSetFromSpanProducer
    {
        public ChannelReader<Span> SpanReader;
        public ChannelWriter<TraceSet> TraceSetWriter;

        public TraceSetFromSpanProducer()
        {
        }

        public async Task StartProcessingAsync()
        {
            string? _currentTraceId = null;
            List<Span> _currentTrace = new();

            while (true)
            {
                try
                {
                    var span = await SpanReader.ReadAsync();

                    if (span == null)
                    {
                        continue;
                    }

                    if (_currentTraceId == null)
                    {
                        _currentTraceId = span.TraceId;
                    }

                    if (span.TraceId == _currentTraceId)
                    {
                        _currentTrace.Add(span);
                        continue;
                    }

                    // got a completed trace
                    var traceSet = CreateTraceSetFromSpans(_currentTrace, new List<string>(new string[] { _currentTraceId }));

                    _currentTraceId = null;
                    _currentTrace.Clear();

                    await TraceSetWriter.WriteAsync(traceSet);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Exception caught, skipping trace set: {e.Message}, {e.StackTrace}");

                    Thread.Sleep(TimeSpan.FromMinutes(1));
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
