using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TraceStoreFiller
{
    internal class TraceSet
    {
        public string TraceSetId { get; set; }
        public DateTime startTime { get; set; }
        public DateTime endTime { get; set; }
        public List<Trace> traces { get; set; }

        public string ToString()
        {
            var ret = new StringBuilder();

            ret.AppendLine($"TraceSetId: {TraceSetId}");
            ret.AppendLine($"StartTime:  {startTime}");
            ret.AppendLine($"Duration:   {endTime - startTime}");

            foreach (var trace in traces)
            {
                ret.Append(trace.ToString());
            }

            return ret.ToString();
        }
    }
}
