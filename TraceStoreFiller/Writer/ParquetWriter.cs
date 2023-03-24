using Kusto.Cloud.Platform.Utils;
using ParquetSharp;
using ParquetSharp.Schema;
using System.Data;

namespace TraceStoreFiller
{
    internal class ParquetWriter
    {
        public int MaxSpansPerFile { get; set; } = 200000;

        private readonly GroupNode _schema;
        private int _chunkCounter;
        private int _spanCounter;
        private DateTime? _earliestInFile;
        private List<Trace> _chunksInBlob = new();

        public Func<List<Trace>, Stream, DateTime, string, string, Task> WriteStream;

        private string _endpoint;
        private string _namespace;

        public ParquetWriter(string endpoint, string namespace_)
        {
            _schema =
                new GroupNode("schema", Repetition.Required, new Node[]
                {
                   new PrimitiveNode("TraceSetId", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                   new PrimitiveNode("TraceId", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                   new PrimitiveNode("RootCloudRole", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray),
                   new PrimitiveNode("RootCloudRoleInstance", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray),
                   new PrimitiveNode("RootNamespace", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                   new PrimitiveNode("RootEndpoint", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                   new PrimitiveNode("RootDataRegion", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                   new PrimitiveNode("RootSuccess", Repetition.Required, LogicalType.Int(bitWidth: 32, isSigned: true), PhysicalType.Int32),
                   new PrimitiveNode("RootOperationName", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                   new PrimitiveNode("DurationInMs", Repetition.Required, LogicalType.Int(bitWidth: 64, isSigned: true), PhysicalType.Int64),
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
                               new PrimitiveNode("CloudRole", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray),
                               new PrimitiveNode("CloudRoleInstance", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray),
                               new PrimitiveNode("Namespace", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                               new PrimitiveNode("Endpoint", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
                               new PrimitiveNode("DataRegion", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray),
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

            _endpoint = endpoint;
            _namespace = namespace_;
        }

        public async Task WriteTrace(Trace trace)
        {
            foreach (var chunk in trace.chunks)
            {
                _spanCounter += chunk.spans.Count;
            }
            _chunkCounter += 1;

            if (_earliestInFile == null || _earliestInFile > trace.startTime)
            {
                _earliestInFile = trace.startTime;
            }

            _chunksInBlob.Add(trace);

            if (_spanCounter <= MaxSpansPerFile)
            {
                return;
            }

            _chunksInBlob.Sort((t1, t2) => t1.TraceId.CompareTo(t2.TraceId));

            var outputStream = new MemoryStream();
            var parquetWriter = new ParquetFileWriter(
                    outputStream,
                    _schema,
                    new WriterPropertiesBuilder()
                        .Compression(Compression.Snappy)
                        .Build(),
                    leaveOpen: true);

            RowGroupWriter rowGroupWriter = parquetWriter.AppendBufferedRowGroup();

            //  0 TraceSetId
            using (LogicalColumnWriter<string> column = rowGroupWriter.Column(0).LogicalWriter<string>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.traceSet.TraceSetId).ToArray(), 0, _chunksInBlob.Count);
            }

            //  1 TraceId
            using (LogicalColumnWriter<string> column = rowGroupWriter.Column(1).LogicalWriter<string>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.TraceId).ToArray(), 0, _chunksInBlob.Count);
            }

            //  2 Root Cloud role name
            using (LogicalColumnWriter<string?> column = rowGroupWriter.Column(2).LogicalWriter<string?>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks[0].cloudRole).ToArray(), 0, _chunksInBlob.Count);
            }

            //  3 Root Cloud role instance
            using (LogicalColumnWriter<string?> column = rowGroupWriter.Column(3).LogicalWriter<string?>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks[0].cloudRoleInstance).ToArray(), 0, _chunksInBlob.Count);
            }

            //  4 Root Namespace
            using (LogicalColumnWriter<string> column = rowGroupWriter.Column(4).LogicalWriter<string>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks[0].Namespace).ToArray(), 0, _chunksInBlob.Count);
            }

            //  5 Root Endpoint
            using (LogicalColumnWriter<string> column = rowGroupWriter.Column(5).LogicalWriter<string>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks[0].Endpoint).ToArray(), 0, _chunksInBlob.Count);
            }

            //  6 Root Region
            using (LogicalColumnWriter<string> column = rowGroupWriter.Column(6).LogicalWriter<string>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks[0].DataRegion).ToArray(), 0, _chunksInBlob.Count);
            }

            //  7 Root Success
            using (LogicalColumnWriter<int> column = rowGroupWriter.Column(7).LogicalWriter<int>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.All(c => c.Success) ? 1 : 0).ToArray(), 0, _chunksInBlob.Count);
            }

            //  8 Root OperationName
            using (LogicalColumnWriter<string> column = rowGroupWriter.Column(8).LogicalWriter<string>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks[0].RootOperationName).ToArray(), 0, _chunksInBlob.Count);
            }

            //  9 DurationMs 
            using (LogicalColumnWriter<long> column = rowGroupWriter.Column(9).LogicalWriter<long>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => (long)(trace.endTime - trace.startTime).Microseconds).ToArray(), 0, _chunksInBlob.Count);
            }

            //  10 Span Id
            using (LogicalColumnWriter<Nested<string>[]> column = rowGroupWriter.Column(10).LogicalWriter<Nested<string>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string>(sp.SpanId))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 11 Span Parent Id
            using (LogicalColumnWriter<Nested<string>[]> column = rowGroupWriter.Column(11).LogicalWriter<Nested<string>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string>(sp.ParentSpanId))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 12 Span Start Time
            using (LogicalColumnWriter<Nested<DateTime>[]> column = rowGroupWriter.Column(12).LogicalWriter<Nested<DateTime>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<DateTime>(sp.StartTime))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 13 Span End Time
            using (LogicalColumnWriter<Nested<DateTime>[]> column = rowGroupWriter.Column(13).LogicalWriter<Nested<DateTime>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<DateTime>(sp.EndTime))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 14 Span Duration
            using (LogicalColumnWriter<Nested<long>[]> column = rowGroupWriter.Column(14).LogicalWriter<Nested<long>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<long>((sp.EndTime - sp.StartTime).Microseconds))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 15 Span Name
            using (LogicalColumnWriter<Nested<string>[]> column = rowGroupWriter.Column(15).LogicalWriter<Nested<string>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string>(sp.Name))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 16 Span Kind
            using (LogicalColumnWriter<Nested<int>[]> column = rowGroupWriter.Column(16).LogicalWriter<Nested<int>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<int>(sp.Kind))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 17 Span Success
            using (LogicalColumnWriter<Nested<int>[]> column = rowGroupWriter.Column(17).LogicalWriter<Nested<int>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<int>(sp.Success ? 1 : 0))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 18 Span Cloud Role
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(18).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string?>(chunk.cloudRole))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 19 Span Cloud role instance
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(19).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string?>(chunk.cloudRoleInstance))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 20 Span Namespace
            using (LogicalColumnWriter<Nested<string>[]> column = rowGroupWriter.Column(20).LogicalWriter<Nested<string>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string>(chunk.Namespace))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 21 Span Endpoint
            using (LogicalColumnWriter<Nested<string>[]> column = rowGroupWriter.Column(21).LogicalWriter<Nested<string>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string>(chunk.Endpoint))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 22 Span Data Region
            using (LogicalColumnWriter<Nested<string>[]> column = rowGroupWriter.Column(22).LogicalWriter<Nested<string>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string>(chunk.DataRegion))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 23 Span Link ToTraceId 
            using (LogicalColumnWriter<Nested<Nested<string>[]>[]> column = rowGroupWriter.Column(23).LogicalWriter<Nested<Nested<string>[]>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<Nested<string>[]>(sp.links.Select(link => new Nested<string>(link.Item1)).ToArray()))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 24 Span Link ToSpanId
            using (LogicalColumnWriter<Nested<Nested<string>[]>[]> column = rowGroupWriter.Column(24).LogicalWriter<Nested<Nested<string>[]>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<Nested<string>[]>(sp.links.Select(link => new Nested<string>(link.Item2)).ToArray()))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 25 Span Attribute Key
            using (LogicalColumnWriter<Nested<string[]>[]> column = rowGroupWriter.Column(25).LogicalWriter<Nested<string[]>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string[]>(sp.attributes.Select(attr => attr.Key).ToArray()))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 26 Span Attribute Value
            using (LogicalColumnWriter<Nested<string[]>[]> column = rowGroupWriter.Column(26).LogicalWriter<Nested<string[]>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string[]>(sp.attributes.Select(attr => attr.Value == null ? string.Empty : attr.Value.ToString()).ToArray()))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 27 Span httpUrl
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(27).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string?>(sp.httpUrl))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 28 Span httpMethod
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(28).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string?>(sp.httpMethod))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 29 Span httpStatusCodeh
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(29).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string?>(sp.httpStatusCode))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 30 Span dbName
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(30).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string?>(sp.dbName))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 31 Span dbSystem
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(31).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string?>(sp.dbSystem))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 32 Span dbStatement
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(32).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string?>(sp.dbStatement))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 33 Span messagingSystem
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(33).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string?>(sp.messagingSystem))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 34 Span messagingDestination
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(34).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string?>(sp.messagingDestination))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            // 35 Span azureResourceProvider
            using (LogicalColumnWriter<Nested<string?>[]> column = rowGroupWriter.Column(35).LogicalWriter<Nested<string?>[]>())
            {
                column.WriteBatch(_chunksInBlob.Select(trace => trace.chunks.SelectMany(chunk => chunk.spans.Select(sp => new Nested<string?>(sp.azureResourceProvider))).ToArray()).ToArray(), 0, _chunksInBlob.Count);
            }

            rowGroupWriter.Dispose();

            parquetWriter.Dispose();

            if (outputStream != null)
            {
                outputStream.Seek(0, SeekOrigin.Begin);
                await WriteStream(_chunksInBlob, outputStream, (DateTime)_earliestInFile, _endpoint, _namespace);
            }

            _earliestInFile = null;
            _spanCounter = 0;
            _chunkCounter = 0;
            _chunksInBlob = new();
        }
    }
}
