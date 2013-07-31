using System.Net.Http;
using System.Web.Http;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	[RoutePrefix("databases/{databaseName}")]
	public class StatisticsController : RavenApiController
	{
		[HttpGet("stats")]
		public HttpResponseMessage Get()
		{
			return GetMessageWithObject(Database.Statistics);			
		}
	}
}
