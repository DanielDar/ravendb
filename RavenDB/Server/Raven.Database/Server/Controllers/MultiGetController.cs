using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Config;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("multi_get")]
	[RoutePrefix("databases/{databaseName}/multi_get")]
	public class MultiGetController : RavenApiController
	{
		private readonly ThreadLocal<bool> recursive = new ThreadLocal<bool>(() => false);

		[HttpPost("")]
		public async Task<HttpResponseMessage> MultiGet()
		{
			if (recursive.Value)
				throw new InvalidOperationException("Nested requests to multi_get are not supported");
			recursive.Value = true;
			try
			{
				var requests = await ReadJsonObjectAsync<GetRequest[]>();
				var results = new GetResponse[requests.Length];

				ExecuteRequests(DatabasesLandlord.SystemConfiguration, results, requests);

				return GetMessageWithObject(results);
			}
			finally
			{
				recursive.Value = false;
			}
		}

		private void ExecuteRequests(InMemoryRavenConfiguration ravenHttpConfiguration, GetResponse[] results,GetRequest[] requests)
		{
			//// Need to create this here to preserve any current TLS data that we have to copy
			//var contexts = requests.Select(request => new MultiGetHttpContext(ravenHttpConfiguration, context, request, TenantId))
			//	.ToArray();
			//if ("yes".Equals(context.Request.QueryString["parallel"], StringComparison.OrdinalIgnoreCase))
			//{
			//	Parallel.For(0, requests.Length, position =>
			//		HandleRequest(requests, results, position, context, ravenHttpConfiguration, contexts)
			//		);
			//}
			//else
			//{
			//	for (var i = 0; i < requests.Length; i++)
			//	{
			//		HandleRequest(requests, results, i, context, ravenHttpConfiguration, contexts);
			//	}
			//}
		}

		//private void HandleRequest(GetRequest[] requests, GetResponse[] results, int i, InMemoryRavenConfiguration ravenHttpConfiguration, MultiGetHttpContext[] contexts)
		//{
		//	var request = requests[i];
		//	if (request == null)
		//		return;
		//	server.HandleActualRequest(contexts[i]);
		//	results[i] = contexts[i].Complete();
		//}
	}

	public class MultiGetWebApi
	{
		
	}
}
