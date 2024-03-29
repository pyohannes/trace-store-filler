﻿using Azure.Storage.Blobs;
using CsvHelper;
using CsvHelper.Configuration;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System.Data;
using System.Globalization;
using System.IO.Compression;
using System.Runtime.InteropServices;
using System.Threading.Channels;

namespace TraceStoreFiller
{
    internal class CSVProducer
    {
        public Func<Task<BinaryData>> GetNextBlobStream;
        public ChannelWriter<Span> SpanWriter;

        public CSVProducer()
        {
        }

        public async Task StartProcessingAsync()
        {
            while (true)
            {
                var data = await GetNextBlobStream();

                var stream = new MemoryStream(data.ToArray());

                using (var unzipper = new GZipStream(stream, CompressionMode.Decompress))
                using (var reader = new StreamReader(unzipper))
                using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
                {
                    await csv.ReadAsync();
                    csv.ReadHeader();

                    while (await csv.ReadAsync())
                    {
                        await SpanWriter.WriteAsync(Span.From(csv));
                    }
                }

            }
        }
    }
}
