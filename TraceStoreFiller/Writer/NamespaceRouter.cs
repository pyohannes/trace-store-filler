using System.Threading.Channels;

namespace TraceStoreFiller
{
    internal class NamespaceRouter
    {

        private Dictionary<(string endpoint, string namespace_), ParquetWriter> _routes = new();
        private ParquetWriter _writer;
        private ParquetWriterFactory _writerFactory;
        private IndexProducer _indexProducer;

        public ChannelReader<TraceSet> TraceSetReader;

        public NamespaceRouter(ParquetWriterFactory writerFactory, IndexProducer indexProducer)
        {
            _indexProducer = indexProducer;
            _writer = writerFactory.GetWriter(indexProducer);
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
                            await _writer.WriteChunk(chunk);
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
