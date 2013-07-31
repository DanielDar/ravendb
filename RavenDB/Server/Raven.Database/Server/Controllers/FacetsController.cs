﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;
using Microsoft.VisualBasic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Database.Queries;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("facets")]
	public class FacetsController : RavenApiController
	{
		[HttpGet("{id}")]
		public async Task<HttpResponseMessage> FacetsGet(string id)
		{
			return await Facets(id, "GET");
		}

		[HttpPost("{id}")]
		public async Task<HttpResponseMessage> FacetsPost(string id)
		{
			return await Facets(id, "POST");
		}

		private async Task<HttpResponseMessage> Facets(string index, string method)
		{
			var indexQuery = GetIndexQuery(Database.Configuration.MaxPageSize);
			var facetStart = GetFacetStart();
			var facetPageSize = GetFacetPageSize();

			var facets = new List<Facet>();

			var etag = new Etag();
			var msg = await TryGetFacets(index, etag, facets, method);
			if(msg.StatusCode != HttpStatusCode.OK)
				return msg;

			if (MatchEtag(etag))
			{
				msg.StatusCode = HttpStatusCode.NotModified;
				return msg;
			}
				
			WriteETag(etag, msg);

			try
			{
				return GetMessageWithObject(Database.ExecuteGetTermsQuery(index, indexQuery, facets, facetStart, facetPageSize));
			}
			catch (Exception ex)
			{
				if (ex is ArgumentException || ex is InvalidOperationException)
				{
					throw new BadRequestException(ex.Message, ex);
				}

				throw;
			}
		}

		private async Task<HttpResponseMessage> TryGetFacets(string index, Etag etag, List<Facet> facets, string method)
		{
			etag = null;
			facets = null;
			switch (method)
			{
				case "GET":
					var facetSetupDoc = GetFacetSetupDoc();
					if (string.IsNullOrEmpty(facetSetupDoc))
					{
						var facetsJson = GetQueryStringValue("facets");
						if (string.IsNullOrEmpty(facetsJson) == false)
							return TryGetFacetsFromString(index, out etag, out facets, facetsJson);
					}

					JsonDocument jsonDocument = Database.Get(facetSetupDoc, null);
					if (jsonDocument == null)
					{
						return GetMessageWithString("Could not find facet document: " + facetSetupDoc, HttpStatusCode.NotFound);
					}

					etag = GetFacetsEtag(jsonDocument, index);

					facets = jsonDocument.DataAsJson.JsonDeserialization<FacetSetup>().Facets;

					if (facets == null || !facets.Any())
					{
						return GetMessageWithString("No facets found in facets setup document:" + facetSetupDoc, HttpStatusCode.NotFound);
					}
					break;
				case "POST":
					return TryGetFacetsFromString(index, out etag, out facets, await ReadStringAsync());
				default:
					return GetMessageWithString("No idea how to handle this request", HttpStatusCode.BadRequest);

			}
			return new HttpResponseMessage(HttpStatusCode.OK);
		}

		private HttpResponseMessage TryGetFacetsFromString(string index, out Etag etag, out List<Facet> facets,string facetsJson)		
		{
			etag = GetFacetsEtag(facetsJson, index);

			facets = JsonConvert.DeserializeObject<List<Facet>>(facetsJson);

			if (facets == null || !facets.Any())
				return GetMessageWithString("No facets found in request body", HttpStatusCode.BadRequest);

			return GetMessageWithObject(null, HttpStatusCode.OK, etag);
		}

		private Etag GetFacetsEtag(JsonDocument jsonDocument, string index)
		{
			return jsonDocument.Etag.HashWith(Database.GetIndexEtag(index, null));
		}

		private Etag GetFacetsEtag(string jsonFacets, string index)
		{
			using (var md5 = MD5.Create())
			{
				var etagBytes = md5.ComputeHash(Database.GetIndexEtag(index, null).ToByteArray().Concat(Encoding.UTF8.GetBytes(jsonFacets)).ToArray());
				return Etag.Parse(etagBytes);
			}
		}

		private string GetFacetSetupDoc()
		{
			return GetQueryStringValue("facetDoc") ?? "";
		}

		private int GetFacetStart()
		{
			int start;
			return int.TryParse(GetQueryStringValue("facetStart"), out start) ? start : 0;
		}

		private int? GetFacetPageSize()
		{
			int pageSize;
			if (int.TryParse(GetQueryStringValue("facetPageSize"), out pageSize))
				return pageSize;
			return null;
		}
	}
}
