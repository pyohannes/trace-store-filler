using Azure.Storage.Blobs;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System.Data;
using System.Runtime.InteropServices;

namespace TraceStoreFiller
{
    class TraceIndex
    {
        public string TraceId;
        public DateTime StartTime;
        public DateTime EndTime;
        public TimeSpan Duration;
    }

    class TraceSetIndex
    {
        public string TraceSetId;
        public DateTime StartTime;
        public DateTime EndTime;
        public TimeSpan Duration;
    }

    class TraceChunkIndex
    {
        public string? cloudRole;
        public string? cloudRoleInstance;
        public string Namespace;
        public string Endpoint;
        public string DataRegion;
        public bool Success;
        public string RootOperationName;

        public string TraceId;
        public string TraceSetId;
        public string BlobPath;
        public long SpanCount;
    }

    internal class IndexProducer
    {
        private List<TraceIndex> _traceIndices = new();
        private List<TraceSetIndex> _traceSetIndices = new();
        private List<TraceChunkIndex> _traceChunkIndices = new();

        private int _chunkCacheSize;

        public KustoIndexWriter _indexWriter;

        public IndexProducer(KustoIndexWriter indexWriter, int chunkCacheSize = 1000)
        {
            _chunkCacheSize = chunkCacheSize;
            _indexWriter = indexWriter;
        }

        public async Task IndexTraceSet(TraceSet traceSet)
        {
            _traceSetIndices.Add(new TraceSetIndex
            {
                TraceSetId = traceSet.TraceSetId,
                StartTime = traceSet.startTime,
                EndTime = traceSet.endTime,
                Duration = traceSet.endTime - traceSet.startTime

            });
        }

        public async Task IndexTrace(Trace trace)
        {
            _traceIndices.Add(new TraceIndex
            {
                TraceId = trace.TraceId,
                StartTime = trace.startTime,
                EndTime = trace.endTime,
                Duration = trace.endTime - trace.startTime
            });
        }

        public async Task IndexTraceChunk(TraceChunk chunk, string blobPath)
        {
            _traceChunkIndices.Add(new TraceChunkIndex
            {
                cloudRole = chunk.cloudRole,
                cloudRoleInstance = chunk.cloudRoleInstance,
                Namespace = chunk.Namespace,
                Endpoint = chunk.Endpoint,
                DataRegion = chunk.DataRegion,
                Success = chunk.Success,
                RootOperationName = chunk.RootOperationName,
                TraceId = chunk.trace.TraceId,
                TraceSetId = chunk.trace.traceSet.TraceSetId,
                BlobPath = blobPath,
                SpanCount = chunk.spans.Count
            });

            if (_traceChunkIndices.Count > _chunkCacheSize)
            {
                await _indexWriter.WriteIndices(_traceSetIndices, _traceIndices, _traceChunkIndices);

                _traceSetIndices = new();
                _traceIndices = new();
                _traceChunkIndices = new();
            }
        }
    }
}
