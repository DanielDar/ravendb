using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Http;
using System.Net.Http;
using Raven.Database.Storage;

namespace Raven.Database.Server.Controllers
{
	public class DebugController :RavenApiController
	{
		[HttpGet]
		public HttpResponseMessage Config()
		{
			var cfg = Database.Configuration;
			//cfg["OAuthTokenKey"] = "<not shown>";

			return GetMessageWithObject(cfg);
		}

		[HttpGet]
		public HttpResponseMessage Changes()
		{
			return GetMessageWithObject(Database.TransportState.DebugStatuses);
		}

		[HttpGet]
		public HttpResponseMessage Docrefs(string id)
		{
			var totalCount = -1;
			List<string> results = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				totalCount = accessor.Indexing.GetCountOfDocumentsReferencing(id);
				results =
					accessor.Indexing.GetDocumentsReferencing(id)
					        .Skip(GetStart())
					        .Take(GetPageSize(Database.Configuration.MaxPageSize))
					        .ToList();
			});

			return GetMessageWithObject(new
			{
				TotalCount = totalCount,
				Results = results
			});
		}

		[HttpGet]
		public HttpResponseMessage List(string id)
		{
			var listName = id;
			var key = Request.RequestUri.ParseQueryString()["key"];
			if (key == null)
				throw new ArgumentException("Key query string variable is mandatory");

			ListItem listItem = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				listItem = accessor.Lists.Read(listName, key);
				if (listItem == null)
					throw new HttpException(400, "Not found");

			});

			if(listItem == null)
				throw new HttpException(400, "Not found");
				
			return GetMessageWithObject(listItem);
		}
	}
}