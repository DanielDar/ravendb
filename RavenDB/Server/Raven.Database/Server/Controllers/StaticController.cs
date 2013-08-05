﻿using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class StaticController : RavenApiController
	{
		[HttpGet("static")]
		public HttpResponseMessage StaticGet()
		{
			var array = Database.GetAttachments(GetStart(),
										   GetPageSize(Database.Configuration.MaxPageSize),
										   GetEtagFromQueryString(),
										   GetQueryStringValue("startsWith"),
										   long.MaxValue);
			
			return GetMessageWithObject(array);
		}

		[HttpGet("static/{*id}")]
		public HttpResponseMessage StaticGet(string id)
		{
			var filename = id;
			var result = new HttpResponseMessage(HttpStatusCode.OK);
			Database.TransactionalStorage.Batch(async _ => // have to keep the session open for reading of the attachment stream
			{
				var attachmentAndHeaders = Database.GetStatic(filename);
				if (attachmentAndHeaders == null)
				{
					result = new HttpResponseMessage(HttpStatusCode.NotFound);
					return;
				}
				if (MatchEtag(attachmentAndHeaders.Etag))
				{
					result = new HttpResponseMessage(HttpStatusCode.NotModified);
					return;
				}
				
				WriteHeaders(attachmentAndHeaders.Metadata, attachmentAndHeaders.Etag, result);
				var stream = attachmentAndHeaders.Data();
				{
					result.Content = new PushStreamContent((stream1, content, arg3) =>
					{
						stream.CopyTo(stream1);
						stream.Dispose();
					});
				//	stream.CopyTo(await Request.Content.ReadAsStreamAsync());
				}
			});

			return result;
		}

		[HttpHead("static/{*id}")]
		public HttpResponseMessage StaticHead(string id)
		{
			var filename = id;
			var result = new HttpResponseMessage(HttpStatusCode.OK);
			Database.TransactionalStorage.Batch(_ => // have to keep the session open for reading of the attachment stream
			{
				var attachmentAndHeaders = Database.GetStatic(filename);
				if (attachmentAndHeaders == null)
				{
					result = new HttpResponseMessage(HttpStatusCode.NotFound);
					return;
				}
				if (MatchEtag(attachmentAndHeaders.Etag))
				{
					result = new HttpResponseMessage(HttpStatusCode.NotModified);
					return;
				}

				
				WriteHeaders(attachmentAndHeaders.Metadata, attachmentAndHeaders.Etag, result);
				//TODO: set length
				//context.Response.ContentLength64 = attachmentAndHeaders.Size;
			});

			return result;
		}

		[HttpPut("static/{*id}")]
		public async Task<HttpResponseMessage> StaticPut(string id)
		{
			var filename = id;

			var newEtag = Database.PutStatic(filename, GetEtag(), await Request.Content.ReadAsStreamAsync(), Request.Headers.FilterHeadersAttachment());

			var msg = new HttpResponseMessage(HttpStatusCode.NoContent);

			WriteETag(newEtag, msg);
			return msg;
		}

		[HttpPost("static/{*id}")]
		public HttpResponseMessage StaticPost(string id)
		{
			var filename = id;
			var newEtagPost = Database.PutStatic(filename, GetEtag(), null, Request.Headers.FilterHeadersAttachment());

			var msg = GetMessageWithObject(newEtagPost);
			WriteETag(newEtagPost, msg);
			return msg;
		}

		[HttpDelete("static/{*id}")]
		public HttpResponseMessage StaticDelete(string id)
		{
			var filename = id;
			Database.DeleteStatic(filename, GetEtag());
			return new HttpResponseMessage(HttpStatusCode.NoContent);
		}
	}
}
