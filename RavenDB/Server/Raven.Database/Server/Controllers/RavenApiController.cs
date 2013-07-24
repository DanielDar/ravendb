using System;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi;
using System.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public abstract class RavenApiController : ApiController
	{
		public string DatabaseName
		{
			get
			{
				var values = Request.GetRouteData().Values;
				if(values.ContainsKey("databaseName"))
					return Request.GetRouteData().Values["databaseName"] as string;
				return null;
			}
		}

		protected DatabasesLandlord DatabasesLandlord
		{
			get { return ((RavenSelfHostConfigurations)Configuration).Landlord; }
		}

		public DocumentDatabase Database
		{
			get { return DatabasesLandlord.GetDatabaseInternal(DatabaseName).Result; }
		}

		public async Task<T> ReadJsonObjectAsync<T>()
		{
			using (var stream = await Request.Content.ReadAsStreamAsync())
			using (var streamReader = new StreamReader(stream, GetRequestEncoding()))
			{
				using (var jsonReader = new JsonTextReader(streamReader))
				{
					var result = JsonExtensions.CreateDefaultJsonSerializer();

					return (T)result.Deserialize(jsonReader, typeof(T));
				}
			}
		}

		public async Task<RavenJObject> ReadJsonAsync()
		{
			using (var stream = await Request.Content.ReadAsStreamAsync())
			using (var streamReader = new StreamReader(stream, GetRequestEncoding()))
			using (var jsonReader = new RavenJsonTextReader(streamReader))
				return RavenJObject.Load(jsonReader);
		}

		private Encoding GetRequestEncoding()
		{
			if(Request.Content.Headers.ContentType == null || string.IsNullOrWhiteSpace(Request.Content.Headers.ContentType.CharSet))
				return Encoding.GetEncoding("ISO-8859-1");
			return Encoding.GetEncoding(Request.Content.Headers.ContentType.CharSet);
		}

		protected bool EnsureSystemDatabase()
		{
			if (DatabasesLandlord.SystemDatabase == Database)
				return true;

			throw new HttpException(400, "The request '" + Request.RequestUri.AbsoluteUri + "' can only be issued on the system database");
		}

		public int GetStart()
		{
			int start;
			int.TryParse(GetQueryString("start"), out start);
			return Math.Max(0, start);
		}

		public  int GetPageSize(int maxPageSize)
		{
			int pageSize;
			if (int.TryParse(GetQueryString("pageSize"), out pageSize) == false || pageSize < 0)
				pageSize = 25;
			if (pageSize > maxPageSize)
				pageSize = maxPageSize;
			return pageSize;
		}

		public bool MatchEtag(Etag etag)
		{
			return EtagHeaderToEtag() == etag;
		}

		internal Etag EtagHeaderToEtag()
		{
			var responseHeader = GetQueryString("If-None-Match");
			if (string.IsNullOrEmpty(responseHeader))
				return Etag.InvalidEtag;

			if (responseHeader[0] == '\"')
				return Etag.Parse(responseHeader.Substring(1, responseHeader.Length - 2));

			return Etag.Parse(responseHeader);
		}

		public string GetQueryString(string key)
		{
			return Request.RequestUri.ParseQueryString()[key];
		}

		public Etag GetEtagFromQueryString()
		{
			var etagAsString = GetQueryString("etag");
			if (etagAsString != null)
			{
				return Etag.Parse(etagAsString);
			}
			return null;
		}

		protected TransactionInformation GetRequestTransaction()
		{
			if (Request.Headers.Contains("Raven-Transaction-Information") == false)
				return null;
			var txInfo = Request.Headers.GetValues("Raven-Transaction-Information").FirstOrDefault();
			if (string.IsNullOrEmpty(txInfo))
				return null;
			var parts = txInfo.Split(new[] { ", " }, StringSplitOptions.RemoveEmptyEntries);
			if (parts.Length != 2)
				throw new ArgumentException("'Raven-Transaction-Information' is in invalid format, expected format is: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx, hh:mm:ss'");
			return new TransactionInformation
			{
				Id = parts[0],
				Timeout = TimeSpan.ParseExact(parts[1], "c", CultureInfo.InvariantCulture)
			};
		}
	}
}