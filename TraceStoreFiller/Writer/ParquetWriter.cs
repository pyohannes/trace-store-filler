using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System.Data;
using System.Runtime.InteropServices;
using ParquetSharp.Schema;
using ParquetSharp;

namespace TraceStoreFiller
{
    internal class ParquetWriter
    {
        public int MaxSpansPerFile { get; set; } = 50000;

        private readonly GroupNode _schema;
        private ParquetFileWriter? _parquetWriter;
        private int _fileIndex = 0;
        private int _chunkCounter;
        private int _spanCounter;
        private Stream? _outputStream;
        private DateTime? _earliestInFile;
        private List<TraceChunk> _chunksInBlob = new();

        public Func<List<TraceChunk>, Stream, DateTime, Task> WriteStream;

        public ParquetWriter()
        {
            _schema =
                new GroupNode("schema", Repetition.Required, new Node[]
                {
                   new PrimitiveNode("TraceSetId", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                   new PrimitiveNode("TraceId", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                   new PrimitiveNode("CloudRole", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray),
                   new PrimitiveNode("CloudRoleInstance", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray),
                   new PrimitiveNode("Namespace", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                   new PrimitiveNode("Endpoint", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                   new PrimitiveNode("DataRegion", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                   new PrimitiveNode("Success", Repetition.Required, LogicalType.Int(bitWidth: 32, isSigned: true), PhysicalType.Int32),
                   new PrimitiveNode("OperationName", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                   new GroupNode("Spans", Repetition.Required, new Node[]
                   {
                       new GroupNode("list", Repetition.Repeated, new Node[]
                       {
                           new GroupNode("element", Repetition.Required, new Node[]
                           {
                               new PrimitiveNode("Id", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                               new PrimitiveNode("ParentId", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                               new PrimitiveNode("StartTime", Repetition.Required, LogicalType.Timestamp(isAdjustedToUtc: true, timeUnit: TimeUnit.Micros), PhysicalType.Int64),
                               new PrimitiveNode("EndTime", Repetition.Required, LogicalType.Timestamp(isAdjustedToUtc: true, timeUnit: TimeUnit.Micros), PhysicalType.Int64),
                               new PrimitiveNode("DurationInMs", Repetition.Required, LogicalType.Int(bitWidth: 64, isSigned: true), PhysicalType.Int64),
                               new PrimitiveNode("Name", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                               new PrimitiveNode("Kind", Repetition.Required, LogicalType.Int(bitWidth: 32, isSigned: true), PhysicalType.Int32),
                               new PrimitiveNode("Success", Repetition.Required, LogicalType.Int(bitWidth: 32, isSigned: true), PhysicalType.Int32),
                               new GroupNode("Links", Repetition.Optional, new Node[]
                               {
                                   new GroupNode("list", Repetition.Repeated, new Node[]
                                   {
                                       new GroupNode("element", Repetition.Required, new Node[]
                                       {
                                           new PrimitiveNode("ToTraceIds", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                                           new PrimitiveNode("ToSpanIds", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray)
                                       })
                                   })
                               }, LogicalType.List()),
                               new GroupNode("Attributes", Repetition.Optional, new Node[]
                               {
                                   new GroupNode("key_value", Repetition.Repeated, new Node[]
                                   {
                                       new PrimitiveNode("key", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                                       new PrimitiveNode("value", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray)
                                   })
                               }, LogicalType.Map()),
                               new PrimitiveNode("httpUrl", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray),
                               new PrimitiveNode("httpMethod", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray),
                               new PrimitiveNode("httpStatusCode", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray),
                               new PrimitiveNode("dbName", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray),
                               new PrimitiveNode("dbSystem", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray),
                               new PrimitiveNode("dbStatement", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray),
                               new PrimitiveNode("messagingSystem", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray),
                               new PrimitiveNode("messagingDestination", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray),
                               new PrimitiveNode("azureResourceProvider", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray),
                           })
                       })
                   }, LogicalType.List())
                });
        }

        private async Task<ParquetFileWriter> GetWriter()
        {
            if (_parquetWriter != null && _spanCounter > MaxSpansPerFile)
            {
                _parquetWriter.Dispose();

                if (_outputStream != null)
                {
                    _outputStream.Seek(0, SeekOrigin.Begin);
                    await WriteStream(_chunksInBlob, _outputStream, (DateTime)_earliestInFile);
                }

                _parquetWriter = null;
                _outputStream = null;
                _chunksInBlob = new();
                _earliestInFile = null;
            }

            if (_parquetWriter == null)
            {
                _outputStream = new MemoryStream();
                _parquetWriter = new ParquetFileWriter(
                    _outputStream,
                    _schema,
                    new WriterPropertiesBuilder()
                        .Compression(Compression.Snappy)
                        .Build(),
                    leaveOpen: true);

                _spanCounter = 0;
                _chunkCounter = 0;
            }

            return _parquetWriter;
        }

        public async Task WriteChunk(TraceChunk chunk)
        {
            _spanCounter += chunk.spans.Count;
            _chunkCounter += 1;

            var writer = await GetWriter();
            RowGroupWriter rowGroupWriter = writer.AppendBufferedRowGroup();

            if (_earliestInFile == null || _earliestInFile > chunk.trace.startTime)
            {
                _earliestInFile = chunk.trace.startTime;
            }


            _chunksInBlob.Add(chunk);

            //  0 TraceSetId
            using (LogicalColumnWriter<string> column = rowGroupWriter.Column(0).LogicalWriter<string>())
            {
                column.WriteBatch(new string[] { chunk.trace.traceSet.TraceSetId }, 0, 1);
            }

            //  1 TraceId
            using (LogicalColumnWriter<string> column = rowGroupWriter.Column(1).LogicalWriter<string>())
            {
                column.WriteBatch(new string[] { chunk.trace.TraceId }, 0, 1);
            }

            //  2 Cloud role name
            using (LogicalColumnWriter<string?> column = rowGroupWriter.Column(2).LogicalWriter<string?>())
            {
                column.WriteBatch(new string?[] { chunk.cloudRole }, 0, 1);
            }

            //  3 Cloud role instance
            using (LogicalColumnWriter<string?> column = rowGroupWriter.Column(3).LogicalWriter<string?>())
            {
                column.WriteBatch(new string?[] { chunk.cloudRoleInstance }, 0, 1);
            }

            //  4 Namespace
            using (LogicalColumnWriter<string> column = rowGroupWriter.Column(4).LogicalWriter<string>())
            {
                column.WriteBatch(new string[] { chunk.Namespace }, 0, 1);
            }

            //  5 Endpoint
            using (LogicalColumnWriter<string> column = rowGroupWriter.Column(5).LogicalWriter<string>())
            {
                column.WriteBatch(new string[] { chunk.Endpoint }, 0, 1);
            }

            //  6 Region
            using (LogicalColumnWriter<string> column = rowGroupWriter.Column(6).LogicalWriter<string>())
            {
                column.WriteBatch(new string[] { chunk.DataRegion }, 0, 1);
            }

            //  7 Success
            using (LogicalColumnWriter<int> column = rowGroupWriter.Column(7).LogicalWriter<int>())
            {
                column.WriteBatch(new int[] { chunk.Success ? 1: 0 }, 0, 1);
            }

            //  8 OperationName
            using (LogicalColumnWriter<string> column = rowGroupWriter.Column(8).LogicalWriter<string>())
            {
                column.WriteBatch(new string[] { chunk.RootOperationName }, 0, 1);
            }

            //  9 Span Id
            using (LogicalColumnWriter<Nested<string>[]> column = rowGroupWriter.Column(9).LogicalWriter<Nested<string>[]>())
            {
                column.WriteBatch(new Nested<string>[][] { chunk.spans.Select(sp => new Nested<string>(sp.SpanId)).ToArray() }, 0, 1);
            }

            // 10 Span Parent Id
            using (LogicalColumnWriter<Nested<string>[]> column = rowGroupWriter.Column(10).LogicalWriter<Nested<string>[]>())
            {
                column.WriteBatch(new Nested<string>[][] { chunk.spans.Select(sp => new Nested<string>(sp.ParentSpanId)).ToArray() }, 0, 1);
            }

            // 11 Span Start Time
            using (LogicalColumnWriter<Nested<DateTime>[]> column = rowGroupWriter.Column(11).LogicalWriter<Nested<DateTime>[]>())
            {
                column.WriteBatch(new Nested<DateTime>[][] { chunk.spans.Select(sp => new Nested<DateTime>(sp.StartTime)).ToArray() }, 0, 1);
            }

            // 12 Span End Time
            using (LogicalColumnWriter<Nested<DateTime>[]> column = rowGroupWriter.Column(12).LogicalWriter<Nested<DateTime>[]>())
            {
                column.WriteBatch(new Nested<DateTime>[][] { chunk.spans.Select(sp => new Nested<DateTime>(sp.EndTime)).ToArray() }, 0, 1);
            }

            // 13 Span Duration
            using (LogicalColumnWriter<Nested<long>[]> column = rowGroupWriter.Column(13).LogicalWriter<Nested<long>[]>())
            {
                column.WriteBatch(new Nested<long>[][] { chunk.spans.Select(sp => new Nested<long>((sp.EndTime - sp.StartTime).Microseconds)).ToArray() }, 0, 1);
            }

            // 14 Span Name
            using (LogicalColumnWriter<Nested<string>[]> column = rowGroupWriter.Column(14).LogicalWriter<Nested<string>[]>())
            {
                column.WriteBatch(new Nested<string>[][] { chunk.spans.Select(sp => new Nested<string>(sp.Name)).ToArray() }, 0, 1);
            }

            // 15 Span Kind
            using (LogicalColumnWriter<Nested<int>[]> column = rowGroupWriter.Column(15).LogicalWriter<Nested<int>[]>())
            {
                column.WriteBatch(new Nested<int>[][] { chunk.spans.Select(sp => new Nested<int>(sp.Kind)).ToArray() }, 0, 1);
            }

            // 16 Span Success
            using (LogicalColumnWriter<Nested<int>[]> column = rowGroupWriter.Column(16).LogicalWriter<Nested<int>[]>())
            {
                column.WriteBatch(new Nested<int>[][] { chunk.spans.Select(sp => new Nested<int>(sp.Success ? 1 : 0)).ToArray() }, 0, 1);
            }

            // 17 Span Link ToTraceId 
            using (LogicalColumnWriter<Nested<Nested<string>[]>[]> column = rowGroupWriter.Column(17).LogicalWriter<Nested<Nested<string>[]>[]>())
            {
                column.WriteBatch(new Nested<Nested<string>[]>[][] { chunk.spans.Select(sp => new Nested<Nested<string>[]>(sp.links.Select(link => new Nested<string>(link.Item1)).ToArray())).ToArray() }, 0, 1);
            }

            // 18 Span Link ToSpanId
            using (LogicalColumnWriter<Nested<Nested<string>[]>[]> column = rowGroupWriter.Column(18).LogicalWriter<Nested<Nested<string>[]>[]>())
            {
                column.WriteBatch(new Nested<Nested<string>[]>[][] { chunk.spans.Select(sp => new Nested<Nested<string>[]>(sp.links.Select(link => new Nested<string>(link.Item2)).ToArray())).ToArray() }, 0, 1);
            }

            // 19 Span Attribute Key
            using (LogicalColumnWriter<Nested<string[]>[]> column = rowGroupWriter.Column(19).LogicalWriter<Nested<string[]>[]>())
            {
                column.WriteBatch(new Nested<string[]>[][] { chunk.spans.Select(sp => new Nested<string[]>(sp.attributes.Select(attr => attr.Key).ToArray())).ToArray() }, 0, 1);
            }

            // 20 Span Attribute Value
            using (LogicalColumnWriter<Nested<string[]>[]> column = rowGroupWriter.Column(20).LogicalWriter<Nested<string[]>[]>())
            {
                column.WriteBatch(new Nested<string[]>[][] { chunk.spans.Select(sp => new Nested<string[]>(sp.attributes.Select(attr => attr.Value == null ?  string.Empty : attr.Value.ToString()).ToArray())).ToArray() }, 0, 1);
            }

            // 21 Span httpUrl
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(21).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(new Nested<string?>[][] { chunk.spans.Select(sp => new Nested<string?>(sp.httpUrl)).ToArray() }, 0, 1);
            }

            // 22 Span httpMethod
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(22).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(new Nested<string?>[][] { chunk.spans.Select(sp => new Nested<string?>(sp.httpMethod)).ToArray() }, 0, 1);
            }

            // 23 Span httpStatusCodeh
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(23).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(new Nested<string?>[][] { chunk.spans.Select(sp => new Nested<string?>(sp.httpStatusCode)).ToArray() }, 0, 1);
            }

            // 24 Span dbName
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(24).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(new Nested<string?>[][] { chunk.spans.Select(sp => new Nested<string?>(sp.dbName)).ToArray() }, 0, 1);
            }

            // 25 Span dbSystem
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(25).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(new Nested<string?>[][] { chunk.spans.Select(sp => new Nested<string?>(sp.dbSystem)).ToArray() }, 0, 1);
            }

            // 26 Span dbStatement
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(26).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(new Nested<string?>[][] { chunk.spans.Select(sp => new Nested<string?>(sp.dbStatement)).ToArray() }, 0, 1);
            }

            // 27 Span messagingSystem
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(27).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(new Nested<string?>[][] { chunk.spans.Select(sp => new Nested<string?>(sp.messagingSystem)).ToArray() }, 0, 1);
            }

            // 28 Span messagingDestination
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(28).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(new Nested<string?>[][] { chunk.spans.Select(sp => new Nested<string?>(sp.messagingDestination)).ToArray() }, 0, 1);
            }

            // 29 Span azureResourceProvider
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(29).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(new Nested<string?>[][] { chunk.spans.Select(sp => new Nested<string?>(sp.azureResourceProvider)).ToArray() }, 0, 1);
            }

            rowGroupWriter.Dispose();
        }
    }
}
