using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Replication.Data;
using Raven.Database.Server.Controllers;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Controllers
{
	public class ReplicationLastEtagController: RavenApiController
	{
		[HttpGet("replication/lastEtag")]
		public HttpResponseMessage ReplicationLastEtagGet()
		{
			var src = GetQueryStringValue("from");
			var dbid = GetQueryStringValue("dbid");
			if (dbid == Database.TransactionalStorage.Id.ToString())
				throw new InvalidOperationException("Both source and target databases have database id = " + dbid + "\r\nDatabase cannot replicate to itself.");

			if (string.IsNullOrEmpty(src))
			{
				return new HttpResponseMessage(HttpStatusCode.BadRequest);
			}

			while (src.EndsWith("/"))
				src = src.Substring(0, src.Length - 1);// remove last /, because that has special meaning for Raven

			if (string.IsNullOrEmpty(src))
			{
				return new HttpResponseMessage(HttpStatusCode.BadRequest);
			}

			using (Database.DisableAllTriggersForCurrentThread())
			{
				var document = Database.Get(Constants.RavenReplicationSourcesBasePath + "/" + src, null);

				SourceReplicationInformation sourceReplicationInformation;

				var serverInstanceId = Database.TransactionalStorage.Id; // this is my id, sent to the remote serve

				if (document == null)
				{
					sourceReplicationInformation = new SourceReplicationInformation()
					{
						Source = src
					};
				}
				else
				{
					sourceReplicationInformation = document.DataAsJson.JsonDeserialization<SourceReplicationInformation>();
					sourceReplicationInformation.ServerInstanceId = serverInstanceId;
				}

				var currentEtag = GetQueryStringValue("currentEtag");
				//TODO: log
				//log.Debug("Got replication last etag request from {0}: [Local: {1} Remote: {2}]", src,
				//		  sourceReplicationInformation.LastDocumentEtag, currentEtag);

				return GetMessageWithObject(sourceReplicationInformation);
			}
		}

		[HttpPut("replication/lastEtag")]
		public HttpResponseMessage ReplicationLastEtagPut()
		{
			var src = GetQueryStringValue("from");
			var dbid = GetQueryStringValue("dbid");
			if (dbid == Database.TransactionalStorage.Id.ToString())
				throw new InvalidOperationException("Both source and target databases have database id = " + dbid + "\r\nDatabase cannot replicate to itself.");

			if (string.IsNullOrEmpty(src))
			{
				return new HttpResponseMessage(HttpStatusCode.BadRequest);
			}

			while (src.EndsWith("/"))
				src = src.Substring(0, src.Length - 1);// remove last /, because that has special meaning for Raven

			if (string.IsNullOrEmpty(src))
			{
				return new HttpResponseMessage(HttpStatusCode.BadRequest);
			}

			using (Database.DisableAllTriggersForCurrentThread())
			{
				var document = Database.Get(Constants.RavenReplicationSourcesBasePath + "/" + src, null);

				SourceReplicationInformation sourceReplicationInformation;

				Etag docEtag = null, attachmentEtag = null;
				try
				{
					docEtag = Etag.Parse(GetQueryStringValue("docEtag"));
				}
				catch
				{

				}
				try
				{
					attachmentEtag = Etag.Parse(GetQueryStringValue("attachmentEtag"));
				}
				catch
				{

				}

				Guid serverInstanceId;
				if (Guid.TryParse(GetQueryStringValue("dbid"), out serverInstanceId) == false)
					serverInstanceId = Database.TransactionalStorage.Id;

				if (document == null)
				{
					sourceReplicationInformation = new SourceReplicationInformation
					{
						ServerInstanceId = serverInstanceId,
						LastAttachmentEtag = attachmentEtag ?? Etag.Empty,
						LastDocumentEtag = docEtag ?? Etag.Empty,
						Source = src
					};
				}
				else
				{
					sourceReplicationInformation = document.DataAsJson.JsonDeserialization<SourceReplicationInformation>();
					sourceReplicationInformation.ServerInstanceId = serverInstanceId;
					sourceReplicationInformation.LastDocumentEtag = docEtag ?? sourceReplicationInformation.LastDocumentEtag;
					sourceReplicationInformation.LastAttachmentEtag = attachmentEtag ?? sourceReplicationInformation.LastAttachmentEtag;
				}

				var etag = document == null ? Etag.Empty : document.Etag;
				var metadata = document == null ? new RavenJObject() : document.Metadata;

				var newDoc = RavenJObject.FromObject(sourceReplicationInformation);

				//TODO: log
				//log.Debug("Updating replication last etags from {0}: [doc: {1} attachment: {2}]", src,
				//				  sourceReplicationInformation.LastDocumentEtag,
				//				  sourceReplicationInformation.LastAttachmentEtag);

				Database.Put(Constants.RavenReplicationSourcesBasePath + "/" + src, etag, newDoc, metadata, null);
			}

			return new HttpResponseMessage(HttpStatusCode.OK);
		}
	}
}
