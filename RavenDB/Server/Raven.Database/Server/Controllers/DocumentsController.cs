using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	[RoutePrefix("databases/{databaseName}")]
	public class DocumentsController : RavenApiController
	{
		[HttpGet("docs")]
		public HttpResponseMessage DocsGet()
		{
			long documentsCount = 0;
			var lastDocEtag = Etag.Empty;
			Database.TransactionalStorage.Batch(accessor =>
			{
				lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag();
				documentsCount = accessor.Documents.GetDocumentsCount();
			});

			lastDocEtag = lastDocEtag.HashWith(BitConverter.GetBytes(documentsCount));
			if (MatchEtag(lastDocEtag))
			{
				return new HttpResponseMessage(HttpStatusCode.NotModified);
			}

			//TODO: write headers
			//WriteHeaders(new RavenJObject(), lastDocEtag);

			var startsWith = GetQueryStringValue("startsWith");
			if (string.IsNullOrEmpty(startsWith))
				return GetMessageWithObject(Database.GetDocuments(GetStart(), GetPageSize(Database.Configuration.MaxPageSize), GetEtagFromQueryString()));
			return GetMessageWithObject(Database.GetDocumentsWithIdStartingWith(startsWith, GetQueryStringValue("matches"),
			                                                                    GetStart(), GetPageSize(Database.Configuration.MaxPageSize)));
		}

		[HttpPost("docs")]
		public async Task<HttpResponseMessage> DocsPut()
		{
			var json = await ReadJsonAsync();
			var id = Database.Put(null, Etag.Empty, json,
								  Request.Content.Headers.FilterHeaders(),
								  GetRequestTransaction());

			
			return GetMessageWithObject(id);
		}
	}
}