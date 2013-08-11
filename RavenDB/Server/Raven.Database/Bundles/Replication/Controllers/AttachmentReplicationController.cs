using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Replication.Data;
using Raven.Bundles.Replication.Plugins;
using Raven.Bundles.Replication.Responders;
using Raven.Database.Server.Controllers;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Controllers
{
	[ExportMetadata("Bundle", "Replication")]
	public class AttachmentReplicationController : RavenApiController
	{
		[HttpGet("attachmentReplication")]
		public async Task<HttpResponseMessage> AttachmentReplicationGet()
		{
			var src = GetQueryStringValue("from");
			if (string.IsNullOrEmpty(src))
				return new HttpResponseMessage(HttpStatusCode.BadRequest);

			while (src.EndsWith("/"))
				src = src.Substring(0, src.Length - 1);// remove last /, because that has special meaning for Raven

			if (string.IsNullOrEmpty(src))
				return new HttpResponseMessage(HttpStatusCode.BadRequest);

			var array = await ReadBsonArrayAsync();
			using (Database.DisableAllTriggersForCurrentThread())
			{
				Database.TransactionalStorage.Batch(actions =>
				{
					var lastEtag = Etag.Empty;
					foreach (RavenJObject attachment in array)
					{
						var metadata = attachment.Value<RavenJObject>("@metadata");
						if (metadata[Constants.RavenReplicationSource] == null)
						{
							// not sure why, old attachment from when the user didn't have replication
							// that we suddenly decided to replicate, choose the source for that
							metadata[Constants.RavenReplicationSource] = RavenJToken.FromObject(src);
						}

						lastEtag = Etag.Parse(attachment.Value<byte[]>("@etag"));
						var id = attachment.Value<string>("@id");

						ReplicateAttachment(actions, id, metadata, attachment.Value<byte[]>("data"), lastEtag, src);
					}

					var replicationDocKey = Constants.RavenReplicationSourcesBasePath + "/" + src;
					var replicationDocument = Database.Get(replicationDocKey, null);
					Etag lastDocId = null;
					if (replicationDocument != null)
					{
						lastDocId =
							replicationDocument.DataAsJson.JsonDeserialization<SourceReplicationInformation>().
								LastDocumentEtag;
					}

					Guid serverInstanceId;
					if (Guid.TryParse(GetQueryStringValue("dbid"), out serverInstanceId) == false)
						serverInstanceId = Database.TransactionalStorage.Id;

					Database.Put(replicationDocKey, null,
								 RavenJObject.FromObject(new SourceReplicationInformation
								 {
									 Source = src,
									 LastDocumentEtag = lastDocId,
									 LastAttachmentEtag = lastEtag,
									 ServerInstanceId = serverInstanceId
								 }),
								 new RavenJObject(), null);
				});
			}

			return new HttpResponseMessage(HttpStatusCode.OK);
		}

		[ImportMany]
		public IEnumerable<AbstractAttachmentReplicationConflictResolver> ReplicationConflictResolvers { get; set; }

		private void ReplicateAttachment(IStorageActionsAccessor actions, string id, RavenJObject metadata, byte[] data, Etag lastEtag, string src)
		{
			new AttachmentReplicationBehavior
			{
				Actions = actions,
				Database = Database,
				ReplicationConflictResolvers = ReplicationConflictResolvers,
				Src = src
			}.Replicate(id, metadata, data);
		}
	}
}
