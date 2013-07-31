using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("transformers")]
	[RoutePrefix("databases/{databaseName}/transformers")]
	public class TransformersController :RavenApiController
	{
		[HttpGet("{id}")]
		public HttpResponseMessage TransformerGet(string id)
		{
			var transformer = id;
			if (string.IsNullOrEmpty(transformer) == false && transformer != "/")
			{
				var transformerDefinition = Database.GetTransformerDefinition(transformer);
				if (transformerDefinition == null)
					return new HttpResponseMessage(HttpStatusCode.NotFound);

				return GetMessageWithObject(RavenJObject.FromObject(transformerDefinition));
			}
			return new HttpResponseMessage();
		}

		[HttpGet("")]
		public HttpResponseMessage TransformerGet()
		{
			var namesOnlyString = GetQueryStringValue("namesOnly");
			bool namesOnly;
			RavenJArray transformers;
			if (bool.TryParse(namesOnlyString, out namesOnly) && namesOnly)
				transformers = Database.GetTransformerNames(GetStart(), GetPageSize(Database.Configuration.MaxPageSize));
			else
				transformers = Database.GetTransformers(GetStart(), GetPageSize(Database.Configuration.MaxPageSize));

			return GetMessageWithObject(transformers);
		}

		[HttpPut("{id}")]
		public async Task<HttpResponseMessage> TransformersPut(string id)
		{
			var transformer = id;
			var data = await ReadJsonObjectAsync<TransformerDefinition>();
			if (data == null || string.IsNullOrEmpty(data.TransformResults))
			{
				return GetMessageWithString("Expected json document with 'TransformResults' property", HttpStatusCode.BadRequest);
			}

			try
			{
				var transformerName = Database.PutTransform(transformer, data);
				return GetMessageWithObject(new { Transformer = transformerName }, HttpStatusCode.Created);
			}
			catch (Exception ex)
			{
				return GetMessageWithObject(new
				{
					Message = ex.Message,
					Error = ex.ToString()
				}, HttpStatusCode.BadRequest);
			}
		}

		[HttpDelete("{id}")]
		public HttpResponseMessage TransformersDelete(string id)
		{
			Database.DeleteTransfom(id);
			return new HttpResponseMessage(HttpStatusCode.NoContent);
		}
	}
}
