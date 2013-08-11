using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Replication.Data;
using Raven.Bundles.Replication.Plugins;
using Raven.Bundles.Replication.Responders;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Server.Controllers;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Controllers
{
	[ExportMetadata("Bundle", "Replication")]
	public class DocumentReplicationController : RavenApiController
	{
		[ImportMany]
		public IEnumerable<AbstractDocumentReplicationConflictResolver> ReplicationConflictResolvers { get; set; }

		private ReplicationTask replicationTask;
		public ReplicationTask ReplicationTask
		{
			get { return replicationTask ?? (replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault()); }
		}

		[HttpGet("documentReplication")]
		public async Task<HttpResponseMessage> DocumentReplicationGet()
		{
			var src = GetQueryStringValue("from");
			if (string.IsNullOrEmpty(src))
				return new HttpResponseMessage(HttpStatusCode.BadRequest);

			while (src.EndsWith("/"))
				src = src.Substring(0, src.Length - 1);// remove last /, because that has special meaning for Raven

			if (string.IsNullOrEmpty(src))
				return new HttpResponseMessage(HttpStatusCode.BadRequest);

			var array = await ReadJsonArrayAsync();
			if (ReplicationTask != null)
				ReplicationTask.HandleHeartbeat(src);

			using (Database.DisableAllTriggersForCurrentThread())
			{
				Database.TransactionalStorage.Batch(actions =>
				{
					var lastEtag = Etag.Empty.ToString();
					foreach (RavenJObject document in array)
					{
						var metadata = document.Value<RavenJObject>("@metadata");
						if (metadata[Constants.RavenReplicationSource] == null)
						{
							// not sure why, old document from when the user didn't have replication
							// that we suddenly decided to replicate, choose the source for that
							metadata[Constants.RavenReplicationSource] = RavenJToken.FromObject(src);
						}
						lastEtag = metadata.Value<string>("@etag");
						var id = metadata.Value<string>("@id");
						document.Remove("@metadata");
						ReplicateDocument(actions, id, metadata, document, src);
					}

					var replicationDocKey = Constants.RavenReplicationSourcesBasePath + "/" + src;
					var replicationDocument = Database.Get(replicationDocKey, null);
					var lastAttachmentId = Etag.Empty;
					if (replicationDocument != null)
					{
						lastAttachmentId =
							replicationDocument.DataAsJson.JsonDeserialization<SourceReplicationInformation>().
								LastAttachmentEtag;
					}

					Guid serverInstanceId;
					if (Guid.TryParse(GetQueryStringValue("dbid"), out serverInstanceId) == false)
						serverInstanceId = Database.TransactionalStorage.Id;

					Database.Put(replicationDocKey, null,
								 RavenJObject.FromObject(new SourceReplicationInformation
								 {
									 Source = src,
									 LastDocumentEtag = Etag.Parse(lastEtag),
									 LastAttachmentEtag = lastAttachmentId,
									 ServerInstanceId = serverInstanceId
								 }),
								 new RavenJObject(), null);
				});
			}

			return new HttpResponseMessage(HttpStatusCode.OK);
		}

		private void ReplicateDocument(IStorageActionsAccessor actions, string id, RavenJObject metadata, RavenJObject document, string src)
		{
			new DocumentReplicationBehavior
			{
				Actions = actions,
				Database = Database,
				ReplicationConflictResolvers = ReplicationConflictResolvers,
				Src = src
			}.Replicate(id, metadata, document);
		}
	}
}
