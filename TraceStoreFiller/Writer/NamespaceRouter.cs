using System.Threading.Channels;

namespace TraceStoreFiller
{
    internal class NamespaceRouter
    {

        private Dictionary<(string endpoint, string namespace_), ParquetWriter> _rootNsWriters = new();
        private ParquetWriterFactory _writerFactory;
        private IndexProducer _indexProducer;

        public ChannelReader<TraceSet> TraceSetReader;

        public NamespaceRouter(ParquetWriterFactory writerFactory, IndexProducer indexProducer)
        {
            _indexProducer = indexProducer;
            _writerFactory = writerFactory;
        }

        public async Task StartRouting(int batchSize = 2)
        {
            while (true)
            {
                try
                {
                    var traceSet = await TraceSetReader.ReadAsync();
                    //await _indexProducer.IndexTraceSet(traceSet);

                    foreach (var trace in traceSet.traces)
                    {
                        if (trace.chunks[0].Namespace != "csmEastUSRPF")
                        {
                            continue;
                        }

                        //await _indexProducer.IndexTrace(trace);

                        var nsIndex = (trace.chunks[0].Endpoint, trace.chunks[0].Namespace);
                        _ = _rootNsWriters.TryGetValue(nsIndex, out var writer);

                        if (writer == null)
                        {
                            writer = _writerFactory.GetWriter(_indexProducer, nsIndex.Endpoint, nsIndex.Namespace);
                            _rootNsWriters[nsIndex] = writer;
                        }

                        await writer.WriteTrace(trace);
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
