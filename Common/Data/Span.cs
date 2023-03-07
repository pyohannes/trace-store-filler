using CsvHelper;
using Newtonsoft.Json.Linq;
using System.Data;
using System.Text;

namespace TraceStoreFiller
{
    public class Span
    {
        public string SpanId { get; set; }
        public string TraceId { get; set; }
        public string ParentSpanId { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public String Namespace { get; set; }
        public String Endpoint { get; set; }
        public bool Success { get; set; }
        public string Name { get; set; }
        public int Kind { get; set; }
        public string DataRegion { get; set; }
        public IDictionary<string, object>? cloudIdentity { get; set; }
        public Dictionary<string, object> attributes { get; set; } = new();
        public List<(string, string)> links { get; set; } = new();

        public string? httpUrl { get; set; }
        public string? azureResourceProvider { get; set; }
        public string? httpStatusCode { get; set; }
        public string? httpMethod { get; set; }
        public string? dbSystem { get; set; }
        public string? dbName { get; set; }
        public string? dbStatement { get; set; }
        public string? messagingSystem { get; set; }
        public string? messagingDestination { get; set; }

        public static Span From(CsvReader reader)
        {
            Span span = new Span();

            span.SpanId = reader.GetField<string>("w3cSpanId");
            span.TraceId = reader.GetField<string>("w3cTraceId");
            span.ParentSpanId = reader.GetField<string>("w3cParentId");
            span.StartTime = reader.GetField<DateTime>("startTime");
            span.EndTime = reader.GetField<DateTime>("endTime");
            span.Namespace = reader.GetField<string>("namespace");
            span.Endpoint = reader.GetField<string>("endpoint");
            span.Success = reader.GetField<bool>("success");
            span.Name = reader.GetField<string>("spanName");
            span.Kind = reader.GetField<int>("spanKind");
            span.DataRegion = reader.GetField<string>("dataRegion");

            var cloudIdentity = reader.GetField<string>("cloudIdentity");
            if (!string.IsNullOrWhiteSpace(cloudIdentity))
            {
                span.cloudIdentity = JObject.Parse(cloudIdentity).ToObject<Dictionary<string, object?>>();
            }

            try
            {
                var attributes = reader.GetField<string>("attributes");
                if (!string.IsNullOrWhiteSpace(attributes)) {
                    foreach (var attr in JObject.Parse(attributes).ToObject<Dictionary<string, object?>>())
                    {
                        switch (attr.Key)
                        {
                            case "httpUrl":
                                span.httpUrl = (string?)attr.Value;
                                break;
                            case "httpMethod":
                                span.httpMethod = (string?)attr.Value;
                                break;
                            case "httpStatusCode":
                                span.httpStatusCode = (string?)attr.Value;
                                break;
                            case "dbName":
                                span.dbName = (string?)attr.Value;
                                break;
                            case "dbSystem":
                                span.dbSystem = (string?)attr.Value;
                                break;
                            case "dbStatement":
                                span.dbStatement = (string?)attr.Value;
                                break;
                            case "messagingSystem":
                                span.messagingSystem = (string?)attr.Value;
                                break;
                            case "messagingDestination":
                                span.messagingDestination = (string?)attr.Value;
                                break;
                            case "azureResourceProvider":
                                span.azureResourceProvider = (string?)attr.Value;
                                break;
                            default:
                                span.attributes[attr.Key] = attr.Value;
                                break;
                        }
                    }
                }
            }
            catch (Exception e)
            {
            }

            try
            {
                var customAttributes = reader.GetField<string>("customAttributes");
                if (!string.IsNullOrWhiteSpace(customAttributes))
                {
                    foreach (var attr in JObject.Parse(customAttributes).ToObject<Dictionary<string, object?>>())
                    {
                        span.attributes[attr.Key] = attr.Value;
                    }
                }
            }
            catch (Exception e)
            {
            }

            return span;
        }

        public static Span From(IDataReader reader)
        {
            Span span = new Span();

            span.TraceId = reader.GetString(0);
            span.SpanId = reader.GetString(1);
            span.ParentSpanId = reader.GetString(2);
            span.StartTime = reader.GetDateTime(12);
            span.EndTime = reader.GetDateTime(20);
            span.Namespace = reader.GetString(7);
            span.Endpoint = reader.GetString(8);
            span.Success = (((SByte)reader.GetValue(14)) != 0);
            span.Name = reader.GetString(10);
            span.Kind = reader.GetInt32(11);
            span.DataRegion = reader.GetString(24);

            try
            {
                var cloudIdentity = (JObject)reader.GetValue(6);
                span.cloudIdentity = cloudIdentity.ToObject<Dictionary<string, object>>();
            }
            catch (Exception e)
            {
                span.cloudIdentity = null;
            }

            try
            {
                var attributes = (JObject)reader.GetValue(16);
                foreach (var attr in attributes.ToObject<Dictionary<string, object>>())
                {
                    switch (attr.Key)
                    {
                        case "httpUrl":
                            span.httpUrl = (string?)attr.Value;
                            break;
                        case "httpMethod":
                            span.httpMethod = (string?)attr.Value;
                            break;
                        case "httpStatusCode":
                            span.httpStatusCode = (string?)attr.Value;
                            break;
                        case "dbName":
                            span.dbName = (string?)attr.Value;
                            break;
                        case "dbSystem":
                            span.dbSystem = (string?)attr.Value;
                            break;
                        case "dbStatement":
                            span.dbStatement = (string?)attr.Value;
                            break;
                        case "messagingSystem":
                            span.messagingSystem = (string?)attr.Value;
                            break;
                        case "messagingDestination":
                            span.messagingDestination = (string?)attr.Value;
                            break;
                        case "azureResourceProvider":
                            span.azureResourceProvider = (string?)attr.Value;
                            break;
                        default:
                            span.attributes[attr.Key] = attr.Value;
                            break;
                    }
                }
            }
            catch (Exception e)
            {
            }

            try
            {
                var customAttributes = (JObject)reader.GetValue(17);
                foreach (var attr in customAttributes.ToObject<Dictionary<string, object>>())
                {
                    span.attributes[attr.Key] = attr.Value;
                }
            }
            catch (Exception e)
            {
            }

            return span;
        }

        public string ToString()
        {
            StringBuilder sb = new StringBuilder();

            sb.AppendLine($"      Name:      {Name}");
            sb.AppendLine($"      Kind:      {Kind}");
            sb.AppendLine($"      SpanId:    {SpanId}");
            sb.AppendLine($"      Parent:    {ParentSpanId}");
            sb.AppendLine($"      StartTime: {StartTime}");
            sb.AppendLine($"      Duration:  {EndTime - StartTime}");
            sb.Append("      Attributes: {");
            sb.Append(string.Join(", ", attributes.Select(attr => $"\"{attr.Key}\": \"{attr.Value}\"")));
            sb.Append("}");

            return sb.ToString();
        }
    }
}
