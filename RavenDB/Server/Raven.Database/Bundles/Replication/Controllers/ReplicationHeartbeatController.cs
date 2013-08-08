using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Server.Controllers;

namespace Raven.Database.Bundles.Replication.Controllers
{
	public class ReplicationHeartbeatController : RavenApiController
	{
		[HttpPost("replication/heartbeat")]
		public HttpResponseMessage ReplicationHeartbeatPost()
		{
			var src = GetQueryStringValue("from");

			var replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();
			if (replicationTask == null)
			{
				return GetMessageWithString("Cannot find replication task setup in the database", HttpStatusCode.NotFound);
			}

			replicationTask.HandleHeartbeat(src);

			return new HttpResponseMessage(HttpStatusCode.OK);
		}
	}
}
