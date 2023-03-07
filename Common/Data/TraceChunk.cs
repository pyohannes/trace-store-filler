using Kusto.Cloud.Platform.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TraceStoreFiller
{
    public class TraceChunk
    {
        public string? cloudRole { get; set; }
        public string? cloudRoleInstance { get; set; }
        public string Namespace { get; set; }
        public string Endpoint { get; set; }
        public string DataRegion { get; set; }
        public bool Success { get; set; }
        public string RootOperationName { get; set; }
        public List<Span> spans { get; set; } = new();

        public Trace trace { get; set; }

        public string ToString()
        {
            StringBuilder sb = new StringBuilder();

            sb.AppendLine($"    RootOperationName: {RootOperationName}");
            sb.AppendLine($"    CloudRole:         {cloudRole}");
            sb.AppendLine($"    CloudRoleInstance: {cloudRoleInstance}");
            sb.AppendLine($"    Namespace:         {Namespace}");
            sb.AppendLine($"    Endpoint:          {Endpoint}");
            sb.AppendLine($"    DataRegion:        {DataRegion}");
            sb.AppendLine($"    Success:           {Success}");
            sb.AppendLine($"    Spans:");

            foreach (var span in spans)
            {
                sb.AppendLine(span.ToString());
            }

            return sb.ToString();
        }
    }
}
