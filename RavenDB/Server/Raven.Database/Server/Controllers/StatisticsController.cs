using System.Net.Http;
using System.Web.Http;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("stats")]
	[RoutePrefix("databases/{databaseName}/stats")]
	public class StatisticsController : RavenApiController
	{
		[HttpGet("")]
		public HttpResponseMessage StatsGet()
		{
			return GetMessageWithObject(Database.Statistics);			
		}
	}
}
