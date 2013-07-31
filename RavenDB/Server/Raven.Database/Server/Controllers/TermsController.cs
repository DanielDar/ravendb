﻿using System.Net;
using System.Net.Http;
using System.Web.Http;
using Lucene.Net.Search;
using Raven.Database.Queries;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	[RoutePrefix("databases/{databaseName}")]
	public class TermsController : RavenApiController
	{
		[HttpGet("terms/{*id}")]
		public HttpResponseMessage TermsGet(string id)
		{
			var index = id;

			var indexEtag = Database.GetIndexEtag(index, null);
			if (MatchEtag(indexEtag))
			{
				return new HttpResponseMessage(HttpStatusCode.NotModified);
			}

			var executeGetTermsQuery = Database.ExecuteGetTermsQuery(index, GetQueryStringValue("field"),
				GetQueryStringValue("fromValue"), GetPageSize(Database.Configuration.MaxPageSize));
			
			var msg = GetMessageWithObject(executeGetTermsQuery);
		
			WriteETag(Database.GetIndexEtag(index, null), msg);
			return msg;
		}
	}
}
