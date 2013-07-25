using System.IO;
using System.Net.Http;
using System.Web.Http;
using Raven.Database.Impl;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("streams")]
	[RoutePrefix("databases/{databaseName}/streams")]
	public class StreamsController : RavenApiController
	{
		[HttpGet("docs")]
		//public HttpResponseMessage StreamDocsGet()
		//{
		//	var result = new HttpRequestMessage();
		//	using (result.Streaming())
		//	{
		//		//context.Response.ContentType = "application/json; charset=utf-8";

		//		using (var writer = new JsonTextWriter(new StreamWriter(context.Response.OutputStream)))
		//		{
		//			writer.WriteStartObject();
		//			writer.WritePropertyName("Results");
		//			writer.WriteStartArray();

		//			Database.TransactionalStorage.Batch(accessor =>
		//			{
		//				var startsWith = GetQueryStringValue("startsWith");
		//				int pageSize = GetPageSize(int.MaxValue);
		//				if (string.IsNullOrEmpty(GetQueryStringValue("pageSize")))
		//					pageSize = int.MaxValue;

		//				// we may be sending a LOT of documents to the user, and most 
		//				// of them aren't going to be relevant for other ops, so we are going to skip
		//				// the cache for that, to avoid filling it up very quickly
		//				using (DocumentCacher.SkipSettingDocumentsInDocumentCache())
		//				{
		//					if (string.IsNullOrEmpty(startsWith))
		//					{
		//						Database.GetDocuments(GetStart(), pageSize, GetEtagFromQueryString(),
		//											  doc => doc.WriteTo(writer));
		//					}
		//					else
		//					{
		//						Database.GetDocumentsWithIdStartingWith(startsWith, GetQueryStringValue("matches"),
		//							GetStart(), pageSize, doc => doc.WriteTo(writer));
		//					}
		//				}
		//			});

		//			writer.WriteEndArray();
		//			writer.WriteEndObject();
		//			writer.Flush();
		//		}
		//	}

		//	return 
		//}
	}
}
