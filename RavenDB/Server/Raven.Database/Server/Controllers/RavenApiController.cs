using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Formatting;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web.Http;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Json;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi;
using System.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public abstract class RavenApiController : ApiController
	{
		public string DatabaseName
		{
			get
			{
				var values = Request.GetRouteData().Values;
				if(values.ContainsKey("databaseName"))
					return Request.GetRouteData().Values["databaseName"] as string;
				return null;
			}
		}

		protected DatabasesLandlord DatabasesLandlord
		{
			get { return ((RavenSelfHostConfigurations)Configuration).Landlord; }
		}

		public DocumentDatabase Database
		{
			get { return DatabasesLandlord.GetDatabaseInternal(DatabaseName).Result; }
		}

		public async Task<T> ReadJsonObjectAsync<T>()
		{
			using (var stream = await Request.Content.ReadAsStreamAsync())
			using (var streamReader = new StreamReader(stream, GetRequestEncoding()))
			{
				using (var jsonReader = new JsonTextReader(streamReader))
				{
					var result = JsonExtensions.CreateDefaultJsonSerializer();

					return (T)result.Deserialize(jsonReader, typeof(T));
				}
			}
		}

		public async Task<RavenJObject> ReadJsonAsync()
		{
			using (var stream = await Request.Content.ReadAsStreamAsync())
			using (var streamReader = new StreamReader(stream, GetRequestEncoding()))
			using (var jsonReader = new RavenJsonTextReader(streamReader))
				return RavenJObject.Load(jsonReader);
		}

		public async Task<RavenJArray> ReadJsonArrayAsync()
		{
			using (var stream = await Request.Content.ReadAsStreamAsync())
			using (var streamReader = new StreamReader(stream, GetRequestEncoding()))
			using (var jsonReader = new RavenJsonTextReader(streamReader))
				return RavenJArray.Load(jsonReader);
		}

		private Encoding GetRequestEncoding()
		{
			if(Request.Content.Headers.ContentType == null || string.IsNullOrWhiteSpace(Request.Content.Headers.ContentType.CharSet))
				return Encoding.GetEncoding("ISO-8859-1");
			return Encoding.GetEncoding(Request.Content.Headers.ContentType.CharSet);
		}

		protected bool EnsureSystemDatabase()
		{
			return DatabasesLandlord.SystemDatabase == Database;
		}

		public int GetStart()
		{
			int start;
			int.TryParse(GetQueryStringValue("start"), out start);
			return Math.Max(0, start);
		}

		public  int GetPageSize(int maxPageSize)
		{
			int pageSize;
			if (int.TryParse(GetQueryStringValue("pageSize"), out pageSize) == false || pageSize < 0)
				pageSize = 25;
			if (pageSize > maxPageSize)
				pageSize = maxPageSize;
			return pageSize;
		}

		public bool MatchEtag(Etag etag)
		{
			return EtagHeaderToEtag() == etag;
		}

		internal Etag EtagHeaderToEtag()
		{
			var responseHeader = GetQueryStringValue("If-None-Match");
			if (string.IsNullOrEmpty(responseHeader))
				return Etag.InvalidEtag;

			if (responseHeader[0] == '\"')
				return Etag.Parse(responseHeader.Substring(1, responseHeader.Length - 2));

			return Etag.Parse(responseHeader);
		}

		public string GetQueryStringValue(string key)
		{
			return Request.GetQueryNameValuePairs().Where(pair => pair.Key == key).Select(pair => pair.Value).FirstOrDefault();
		}

		public string[] GetQueryStringValues(string key)
		{
			return Request.GetQueryNameValuePairs().Where(pair => pair.Key == key).Select(pair => pair.Value).ToArray();			
		}

		public Etag GetEtagFromQueryString()
		{
			var etagAsString = GetQueryStringValue("etag");
			if (etagAsString != null)
			{
				return Etag.Parse(etagAsString);
			}
			return null;
		}

		protected TransactionInformation GetRequestTransaction()
		{
			if (Request.Headers.Contains("Raven-Transaction-Information") == false)
				return null;
			var txInfo = Request.Headers.GetValues("Raven-Transaction-Information").FirstOrDefault();
			if (string.IsNullOrEmpty(txInfo))
				return null;
			var parts = txInfo.Split(new[] { ", " }, StringSplitOptions.RemoveEmptyEntries);
			if (parts.Length != 2)
				throw new ArgumentException("'Raven-Transaction-Information' is in invalid format, expected format is: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx, hh:mm:ss'");
			return new TransactionInformation
			{
				Id = parts[0],
				Timeout = TimeSpan.ParseExact(parts[1], "c", CultureInfo.InvariantCulture)
			};
		}

		protected IndexQuery GetIndexQuery(int maxPageSize)
		{
			var query = new IndexQuery
			{
				Query = GetQueryStringValue("query") ?? "",
				Start = GetStart(),
				Cutoff = GetCutOff(),
				CutoffEtag = GetCutOffEtag(),
				PageSize = GetPageSize(maxPageSize),
				SkipTransformResults = GetSkipTransformResults(),
				FieldsToFetch = GetQueryStringValues("fetch"),
				GroupBy = GetQueryStringValues("groupBy"),
				DefaultField = GetQueryStringValue("defaultField"),

				DefaultOperator =
					string.Equals(GetQueryStringValue("operator"), "AND", StringComparison.OrdinalIgnoreCase) ?
						QueryOperator.And :
						QueryOperator.Or,

				AggregationOperation = GetAggregationOperation(),
				SortedFields = GetQueryStringValues("sort")
					.EmptyIfNull()
					.Select(x => new SortedField(x))
					.ToArray(),
				HighlightedFields = GetHighlightedFields().ToArray(),
				HighlighterPreTags = GetQueryStringValues("preTags"),
				HighlighterPostTags = GetQueryStringValues("postTags"),
				ResultsTransformer = GetQueryStringValue("resultsTransformer"),
				QueryInputs = ExtractQueryInputs()
			};


			var spatialFieldName = GetQueryStringValue("spatialField") ?? Constants.DefaultSpatialFieldName;
			var queryShape = GetQueryStringValue("queryShape");
			SpatialUnits units;
			bool unitsSpecified = Enum.TryParse(GetQueryStringValue("spatialUnits"), out units);
			double distanceErrorPct;
			if (!double.TryParse(GetQueryStringValue("distErrPrc"), out distanceErrorPct))
				distanceErrorPct = Constants.DefaultSpatialDistanceErrorPct;
			SpatialRelation spatialRelation;
			if (Enum.TryParse(GetQueryStringValue("spatialRelation"), false, out spatialRelation)
				&& !string.IsNullOrWhiteSpace(queryShape))
			{
				return new SpatialIndexQuery(query)
				{
					SpatialFieldName = spatialFieldName,
					QueryShape = queryShape,
					RadiusUnitOverride = unitsSpecified ? units : (SpatialUnits?)null,
					SpatialRelation = spatialRelation,
					DistanceErrorPercentage = distanceErrorPct,
				};
			}
			return query;
		}

		public Etag GetCutOffEtag()
		{
			var etagAsString = GetQueryStringValue("cutOffEtag");
			if (etagAsString != null)
			{
				etagAsString = Uri.UnescapeDataString(etagAsString);

				return Etag.Parse(etagAsString);
			}
			return null;
		}

		public DateTime? GetCutOff()
		{
			var etagAsString = GetQueryStringValue("cutOff");
			if (etagAsString != null)
			{
				etagAsString = Uri.UnescapeDataString(etagAsString);

				DateTime result;
				if (DateTime.TryParseExact(etagAsString, "o", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out result))
					return result;
				throw new BadRequestException("Could not parse cut off query parameter as date");
			}
			return null;
		}

		public bool GetSkipTransformResults()
		{
			bool result;
			bool.TryParse(GetQueryStringValue("skipTransformResults"), out result);
			return result;
		}

		public AggregationOperation GetAggregationOperation()
		{
			var aggAsString = GetQueryStringValue("aggregation");
			if (aggAsString == null)
			{
				return AggregationOperation.None;
			}

			return (AggregationOperation)Enum.Parse(typeof(AggregationOperation), aggAsString, true);
		}

		public IEnumerable<HighlightedField> GetHighlightedFields()
		{
			var highlightedFieldStrings = GetQueryStringValues("highlight").EmptyIfNull();
			var fields = new HashSet<string>();

			foreach (var highlightedFieldString in highlightedFieldStrings)
			{
				HighlightedField highlightedField;
				if (HighlightedField.TryParse(highlightedFieldString, out highlightedField))
				{
					if (!fields.Add(highlightedField.Field))
						throw new BadRequestException("Duplicate highlighted field has found: " + highlightedField.Field);

					yield return highlightedField;
				}
				else
					throw new BadRequestException(
						"Could not parse highlight query parameter as field highlight options");
			}
		}

		public Dictionary<string, RavenJToken> ExtractQueryInputs()
		{
			var result = new Dictionary<string, RavenJToken>();
			foreach (var key in Request.GetQueryNameValuePairs().Select(pair => pair.Key))
			{
				if (string.IsNullOrEmpty(key)) continue;
				if (key.StartsWith("qp-"))
				{
					var realkey = key.Substring(3);
					result[realkey] = GetQueryStringValue(key);
				}
			}
			return result;
		}

		//public void WriteETag(Etag etag)
		//{
		//	WriteETag(etag.ToString());
		//}

		//public void WriteETag(string etag)
		//{

		//	string clientVersion = null;
		//	if(Request.Headers.Contains("Raven-Client-Version"))
		//		clientVersion = Request.Headers.GetValues("Raven-Client-Version").FirstOrDefault();

		//	if (string.IsNullOrEmpty(clientVersion))
		//	{
		//		HttpContext.Current.Response.AppendHeader("ETag", etag);
		//		return;
		//	}

		//	HttpContext.Current.Response.AppendHeader("ETag", "\"" + etag + "\"");
		//}

		//public void WriteHeaders(RavenJObject headers, Etag etag)
		//{
		//	foreach (var header in headers)
		//	{
		//		if (header.Key.StartsWith("@"))
		//			continue;

		//		switch (header.Key)
		//		{
		//			case "Content-Type":
		//				HttpContext.Current.Response.ContentType = header.Value.Value<string>();
		//				break;
		//			default:
		//				if (header.Value.Type == JTokenType.Date)
		//				{
		//					var rfc1123 = GetDateString(header.Value, "r");
		//					var iso8601 = GetDateString(header.Value, "o");
		//					HttpContext.Current.Response.AddHeader(header.Key, rfc1123);
		//					if (header.Key.StartsWith("Raven-") == false)
		//					{
		//						HttpContext.Current.Response.AddHeader("Raven-" + header.Key, iso8601);
		//					}
		//				}
		//				else
		//				{
		//					var value = UnescapeStringIfNeeded(header.Value.ToString(Formatting.None));
		//					HttpContext.Current.Response.AddHeader(header.Key, value);
		//				}
		//				break;
		//		}
		//	}
		//	if (headers["@Http-Status-Code"] != null)
		//	{
		//		HttpContext.Current.Response.StatusCode = headers.Value<int>("@Http-Status-Code");
		//		HttpContext.Current.Response.StatusDescription = headers.Value<string>("@Http-Status-Description");
		//	}
			
		//	WriteETag(etag);
		//}

		private string GetDateString(RavenJToken token, string format)
		{
			var value = token as RavenJValue;
			if (value == null)
				return token.ToString();

			var obj = value.Value;

			if (obj is DateTime)
				return ((DateTime)obj).ToString(format);

			if (obj is DateTimeOffset)
				return ((DateTimeOffset)obj).ToString(format);

			return obj.ToString();
		}

		private static string UnescapeStringIfNeeded(string str)
		{
			if (str.StartsWith("\"") && str.EndsWith("\""))
				str = Regex.Unescape(str.Substring(1, str.Length - 2));
			if (str.Any(ch => ch > 127))
			{
				// contains non ASCII chars, needs encoding
				return Uri.EscapeDataString(str);
			}
			return str;
		}

		protected HttpResponseMessage GetMessageWithObject(object item, HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
		{
			//TODO: add etag
			return new HttpResponseMessage(code)
			{
				Content = new ObjectContent(typeof(object), item, new JsonMediaTypeFormatter())
			};
		}

		protected HttpResponseMessage GetMessageWithString(string msg, HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
		{
			//TODO: add etag
			return new HttpResponseMessage(code)
			{
				Content = new StringContent(msg)
			};
		}

		private static readonly Encoding DefaultEncoding = new UTF8Encoding(false);
		public void WriteData(RavenJObject data, RavenJObject headers, Etag etag)
		{
			var str = data.ToString(Formatting.None);
			var jsonp = GetQueryStringValue("jsonp");
			if (string.IsNullOrEmpty(jsonp) == false)
			{
				str = jsonp + "(" + str + ");";
				//TODO: header
				//context.Response.AddHeader("Content-Type", "application/javascript; charset=utf-8");
			}
			else
			{
				//TODO: header
				//context.Response.AddHeader("Content-Type", "application/json; charset=utf-8");
			}
			WriteData(DefaultEncoding.GetBytes(str), headers, etag);
		}

		public void WriteData(byte[] data, RavenJObject headers, Etag etag)
		{
			//TODO: header
			//context.WriteHeaders(headers, etag);
			//Response.OutputStream.Write(data, 0, data.Length);
		}

		public Etag GetEtag()
		{
			var etagAsString = GetHeader("If-None-Match") ?? GetHeader("If-Match");
			if (etagAsString != null)
			{
				// etags are usually quoted
				if (etagAsString.StartsWith("\"") && etagAsString.EndsWith("\""))
					etagAsString = etagAsString.Substring(1, etagAsString.Length - 2);

				Etag result;
				if (Etag.TryParse(etagAsString, out result))
					return result;
				throw new BadRequestException("Could not parse If-None-Match or If-Match header as Guid");
			}
			return null;
		}

		private string GetHeader(string key)
		{
			if (Request.Headers.Contains(key) == false)
				return null;
			return Request.Headers.GetValues(key).FirstOrDefault();
		}
	}
}