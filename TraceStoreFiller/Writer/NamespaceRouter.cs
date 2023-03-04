using System.Threading.Channels;

namespace TraceStoreFiller
{
    internal class NamespaceRouter
    {

        private Dictionary<(string endpoint, string namespace_), ParquetWriter> _routes = new();
        private ParquetWriterFactory _writerFactory;
        private IndexProducer _indexProducer;

        public ChannelReader<TraceSet> TraceSetReader;

        public NamespaceRouter(ParquetWriterFactory writerFactory, IndexProducer indexProducer)
        {
            _writerFactory = writerFactory;

            _indexProducer = indexProducer;
        }

        public async Task StartRouting(int batchSize = 2)
        {
            while (true)
            {
                try
                {
                    var traceSet = await TraceSetReader.ReadAsync();
                    await _indexProducer.IndexTraceSet(traceSet);

                    foreach (var trace in traceSet.traces)
                    {
                        await _indexProducer.IndexTrace(trace);

                        foreach (var chunk in trace.chunks)
                        {
                            if (!_routes.ContainsKey((chunk.Endpoint, chunk.Namespace)))
                            {
                                _routes[(chunk.Endpoint, chunk.Namespace)] = _writerFactory.GetWriter(chunk.Endpoint, chunk.Namespace, _indexProducer);
                            }
                            var writer = _routes[(chunk.Endpoint, chunk.Namespace)];

                            await writer.WriteChunk(chunk);
                        }
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
