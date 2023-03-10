// See https://aka.ms/new-console-template for more information
using System.Threading.Channels;
using TraceStoreFiller;


/*var traceIdProducer = new TraceIdProducer(
    "https://cpkustoprodeus.eastus.kusto.windows.net",
    new DateTime(2023, 2, 26, 11, 0, 0),
    new DateTime(2023, 2, 26, 12, 0, 0));

var duplicateTraceIdFilter = new DuplicateTraceIdFilter("https://tracestoreindexeus.eastus.kusto.windows.net/");
duplicateTraceIdFilter.TraceIdProducer = traceIdProducer.GetTraceIdsAsync;

var traceSetIdChannel = Channel.CreateBounded<List<string>>(100);
var traceSetChannel = Channel.CreateBounded<TraceSet>(100);

var traceSetIdsProducer = new TraceSetIdsProducer(
    "https://cpkustoprodeus.eastus.kusto.windows.net");
traceSetIdsProducer.TraceIdProducer = duplicateTraceIdFilter.GetTraceIdsAsync;
traceSetIdsProducer.TraceSetIdWriter = traceSetIdChannel.Writer;

var traceSetProducers = new List<TraceSetProducer>();
for (int i = 0; i < 10; i++)
{
    var traceSetProducer = new TraceSetProducer(
        "https://cpkustoprodeus.eastus.kusto.windows.net");
    traceSetProducer.TraceSetWriter = traceSetChannel.Writer;
    traceSetProducer.TraceSetIdReader = traceSetIdChannel.Reader;
    traceSetProducers.Add(traceSetProducer);
}
*/

var blobReader = new ExportedBlobReader(
    "DefaultEndpointsProtocol=https;AccountName=tracelakeeus;AccountKey=1VBGMao9Nme2o7PzlwWYsZj1fWp7g2eULtoIlKLslOZ1GaKANqrP1HnU4/UK0g8Xn03O86WV8MRv+ASt/JZUHw==;EndpointSuffix=core.windows.net",
    new string[] {
        "0002",
        "0003",
        "0004",
        "0005",
        "0006",
        "0008",
        "0009",
        "0009",
        "0010",
        "0011",
        "0012",
        "0013",
        "0014",
        "0015",
        "0016",
        "0017",
        "0018",
        "0019",
        "001a",
        "001b",
        "001c",
        "001d",
        "001e",
        "001f",
    });

var spanChannel = Channel.CreateBounded<Span>(100000);
var traceSetChannel = Channel.CreateBounded<TraceSet>(1000);
var filteredTraceSetChannel = Channel.CreateBounded<TraceSet>(1000);

var csvProducer = new CSVProducer();
csvProducer.GetNextBlobStream = blobReader.GetNextBlobStream;
csvProducer.SpanWriter = spanChannel.Writer;

var traceSetProducers = new List<TraceSetFromSpanProducer>();
var duplicateTraceFilters = new List<DuplicateTraceIdFilter>();

for (int i = 0; i < 5; i++)
{
    var traceSetProducer = new TraceSetFromSpanProducer();
    traceSetProducer.SpanReader = spanChannel.Reader;
    traceSetProducer.TraceSetWriter = traceSetChannel.Writer;
    traceSetProducers.Add(traceSetProducer);

    var duplicateTraceIdFilter = new DuplicateTraceIdFilter("https://tracestoreindexeus.eastus.kusto.windows.net/");
    duplicateTraceIdFilter.UnfilteredTraceSets = traceSetChannel.Reader;
    duplicateTraceIdFilter.FilteredTraceSets = filteredTraceSetChannel.Writer;
    duplicateTraceFilters.Add(duplicateTraceIdFilter);
}

var writerFactory = new ParquetWriterFactory("DefaultEndpointsProtocol=https;AccountName=tracelakeeus;AccountKey=1VBGMao9Nme2o7PzlwWYsZj1fWp7g2eULtoIlKLslOZ1GaKANqrP1HnU4/UK0g8Xn03O86WV8MRv+ASt/JZUHw==;EndpointSuffix=core.windows.net");

var namespaceRouters = new List<NamespaceRouter>();
for (int i = 0; i < 2; i++)
{
    var indexWriter = new KustoIndexWriter("https://ingest-tracestoreindexeus.eastus.kusto.windows.net/");
    var indexProducer = new IndexProducer(indexWriter);

    var namespaceRouter = new NamespaceRouter(writerFactory, indexProducer);
    namespaceRouter.TraceSetReader = filteredTraceSetChannel.Reader;

    namespaceRouters.Add(namespaceRouter);
}

await Task.WhenAll(
    csvProducer.StartProcessingAsync(),
    Task.WhenAll(duplicateTraceFilters.Select(p => p.StartProcessingAsync())),
    Task.WhenAll(traceSetProducers.Select(p => p.StartProcessingAsync())),
    Task.WhenAll(namespaceRouters.Select(r => r.StartRouting())));
