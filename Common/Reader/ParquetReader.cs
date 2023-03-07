using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System.Data;
using System.Runtime.InteropServices;
using ParquetSharp.Schema;
using ParquetSharp;

namespace TraceStoreFiller
{
    public class ParquetReader
    {

        private ParquetFileReader? _parquetReader;

        public ParquetReader(BinaryData data)
        {
            _parquetReader = new ParquetFileReader(data.ToStream(), true);
        }

        public Trace FindTraceForId(string traceId)
        {
            var trace = new Trace();

            for (int rowGroup = 0; rowGroup < _parquetReader.FileMetaData.NumRowGroups; ++rowGroup)
            {
                using var rowGroupReader = _parquetReader.RowGroup(rowGroup);
                long groupNumRows = rowGroupReader.MetaData.NumRows;

                string[] traceIds = rowGroupReader.Column(1).LogicalReader<string>().ReadAll((int)groupNumRows);

                for (int i = 0; i < traceIds.Length; i++)
                {
                    if (traceIds[i] == traceId)
                    {
                        var chunk = new TraceChunk();
                        chunk.Namespace = rowGroupReader.Column(4).LogicalReader<string>().ElementAt<string>(i);
                        chunk.Endpoint = rowGroupReader.Column(5).LogicalReader<string>().ElementAt<string>(i);

                        trace.chunks.Add(chunk);
                    }
                }

            }

            return trace;
        }
    }
}
