using System.Diagnostics;
using System.Reflection.Metadata;
using TraceStoreFiller;

namespace TraceStoreQuery
{
    internal class FindByTraceIdQuery : IQuery
    {
        public string Name { get; }
        public string Description { get; }

        private QueryExecutor _queryExecutor;
        private TraceStoreFiller.BlobReader _blobReader;

        public FindByTraceIdQuery(QueryExecutor queryExecutor, TraceStoreFiller.BlobReader blobReader)
        {
            Name = "FindByTraceId";
            Description = "Return a complete single trace by its id";

            _queryExecutor = queryExecutor;
            _blobReader = blobReader;
        }

        public void Run(int count)
        {
            List<TimeSpan> overallDuration = new();

            foreach (var id in GetRandomTraceIds(count))
            {
                Stopwatch sw = Stopwatch.StartNew();

                var query =
                    "TraceChunkIndex " +
                    $"| where TraceId == \"{id}\"" +
                    "| project BlobPath";

                string blobPath = null;
                using (var reader = _queryExecutor.ExecuteQuery(query, "traceindexv1"))
                {
                    while (reader.Read())
                    {
                        blobPath = reader.GetString(0);
                    }
                }

                Console.WriteLine($"Querying Kusto: {sw.ElapsedMilliseconds} ms");

                var data = _blobReader.Read(blobPath);

                Console.WriteLine($"Downloading Blob: {sw.ElapsedMilliseconds} ms");

                var pReader = new ParquetReader(data);

                var trace = pReader.FindTraceForId(id);

                Console.WriteLine($"Total elapsed: {sw.ElapsedMilliseconds} ms");
            }
        }

        private List<string> GetRandomTraceIds(int count)
        {
            var ids = new List<string>();


            var query =
                "TraceIndex " +
                $"| sample {count} " +
                "| project TraceId";

            using (var reader = _queryExecutor.ExecuteQuery(query, "traceindexv1"))
            {
                while (reader.Read())
                {
                    ids.Add(reader.GetString(0));
                }
            }

            return ids;
        }
    }
}
