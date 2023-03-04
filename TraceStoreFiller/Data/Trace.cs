using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TraceStoreFiller
{
    internal class Trace
    {
        public string TraceId { get; set; }
        public DateTime startTime { get; set; }
        public DateTime endTime { get; set; }
        public List<TraceChunk> chunks { get; set; }

        public TraceSet traceSet { get; set; } 

        public string ToString()
        {
            StringBuilder sb = new StringBuilder();

            sb.AppendLine($"  TraceId:   {TraceId}");
            sb.AppendLine($"  StartTime: {startTime}");
            sb.AppendLine($"  Duration:  {endTime - startTime}");
            sb.AppendLine($"  Chunks: ");

            foreach (var chunk in chunks)
            {
                sb.AppendLine(chunk.ToString());
            }

            return sb.ToString();
        }
    }
}
