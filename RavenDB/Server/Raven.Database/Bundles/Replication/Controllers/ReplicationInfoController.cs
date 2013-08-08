using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Server.Controllers;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Controllers
{
	public class ReplicationInfoController : RavenApiController
	{
		[HttpGet("replication/info")]
		[HttpPost("replication/info")]
		public HttpResponseMessage ReplicationInfo()
		{
			var mostRecentDocumentEtag = Etag.Empty;
			var mostRecentAttachmentEtag = Etag.Empty;

			Database.TransactionalStorage.Batch(accessor =>
			{
				mostRecentDocumentEtag = accessor.Staleness.GetMostRecentDocumentEtag();
				mostRecentAttachmentEtag = accessor.Staleness.GetMostRecentAttachmentEtag();
			});

			var replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();
			var replicationStatistics = new ReplicationStatistics
			{
				Self = Database.ServerUrl,
				MostRecentDocumentEtag = mostRecentDocumentEtag,
				MostRecentAttachmentEtag = mostRecentAttachmentEtag,
				Stats = replicationTask == null ? new List<DestinationStats>() : replicationTask.DestinationStats.Values.ToList()
			};

			return GetMessageWithObject(RavenJObject.FromObject(replicationStatistics));
		}
	}
}
