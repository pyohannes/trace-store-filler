// See https://aka.ms/new-console-template for more information

using TraceStoreFiller;
using TraceStoreQuery;

var queryExecutor = new QueryExecutor("https://tracestoreindexeus.eastus.kusto.windows.net/");
var blobReader = new TraceStoreFiller.BlobReader("DefaultEndpointsProtocol=https;AccountName=tracelakeeus;AccountKey=1VBGMao9Nme2o7PzlwWYsZj1fWp7g2eULtoIlKLslOZ1GaKANqrP1HnU4/UK0g8Xn03O86WV8MRv+ASt/JZUHw==;EndpointSuffix=core.windows.net");
var query = new FindByTraceIdQuery(queryExecutor, blobReader);

query.Run(2);

