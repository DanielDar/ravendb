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
		public object Config()
		{
			var cfg = Database.Configuration;
			//cfg["OAuthTokenKey"] = "<not shown>";

			return cfg;
		}

		[HttpGet]
		public object Changes()
		{
			return Database.TransportState.DebugStatuses;
		}

		[HttpGet]
		public object Docrefs(string id)
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

			return new
			{
				TotalCount = totalCount,
				Results = results
			};
		}

		[HttpGet]
		public object List(string id)
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
				
			return listItem;
		}
	}
}