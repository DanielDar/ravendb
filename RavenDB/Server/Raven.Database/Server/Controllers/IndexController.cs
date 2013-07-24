using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Formatting;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Database.Data;
using Raven.Database.Queries;
using Raven.Database.Server.Responders;
using Raven.Database.Storage;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("Indexes")]
	public class IndexController : RavenApiController
	{
		[HttpGet("{id}")]
		public HttpResponseMessage IndexGet(string id)
		{
			var index = id;
			if (string.IsNullOrEmpty(GetQueryStringValue("definition")) == false)
				return GetIndexDefinition(index);

			if (string.IsNullOrEmpty(GetQueryStringValue("source")) == false)
				return GetIndexSource(index);

			if (string.IsNullOrEmpty(GetQueryStringValue("debug")) == false)
				return DebugIndex(index);

			if (string.IsNullOrEmpty(GetQueryStringValue("explain")) == false)
				return GetExplanation(index);
			
			return GetIndexQueryResult(index);
		}

		[HttpPut("{id}")]
		public async Task<HttpResponseMessage> IndexPut(string id)
		{
			var index = id;
			var data = await ReadJsonObjectAsync<IndexDefinition>();
			if (data == null || (data.Map == null && (data.Maps == null || data.Maps.Count == 0)))
				return GetMessageWithString("Expected json document with 'Map' or 'Maps' property", HttpStatusCode.BadRequest);

			try
			{
				Database.PutIndex(index, data);
				return GetMessageWithObject(new {Index = index}, HttpStatusCode.Created);
			}
			catch (Exception ex)
			{
				var compilationException = ex as IndexCompilationException;

				return GetMessageWithObject(new
				{
					Message = ex.Message,
					IndexDefinitionProperty = compilationException != null ? compilationException.IndexDefinitionProperty : "",
					ProblematicText = compilationException != null ? compilationException.ProblematicText : "",
					Error = ex.ToString()
				}, HttpStatusCode.BadRequest);
			}
		}

		private HttpResponseMessage GetIndexDefinition(string index)
		{
			var indexDefinition = Database.GetIndexDefinition(index);
			if (indexDefinition == null)
				return new HttpResponseMessage(HttpStatusCode.NotFound);

			indexDefinition.Fields = Database.GetIndexFields(index);

			return GetMessageWithObject(new
			{
				Index = indexDefinition,
			});
		}

		private HttpResponseMessage GetIndexSource(string index)
		{
			var viewGenerator = Database.IndexDefinitionStorage.GetViewGenerator(index);
			if (viewGenerator == null)
				return new HttpResponseMessage(HttpStatusCode.NotFound);

			return GetMessageWithObject(viewGenerator.SourceCode);
		}

		private HttpResponseMessage DebugIndex(string index)
		{
			switch (GetQueryStringValue("debug").ToLowerInvariant())
			{
				case "map":
					return GetIndexMappedResult(index);
				case "reduce":
					return GetIndexReducedResult(index);
				case "schedules":
					return GetIndexScheduledReduces(index);
				case "keys":
					return GetIndexKeysStats(index);
				case "entries":
					return GetIndexEntries(index);
				case "stats":
					return GetIndexStats(index);
				default:
					return GetMessageWithString("Unknown debug option " + GetQueryStringValue("debug"), HttpStatusCode.BadRequest);
			}
		}

		private HttpResponseMessage GetExplanation(string index)
		{
			var dynamicIndex = index.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) ||
							   index.Equals("dynamic", StringComparison.OrdinalIgnoreCase);

			if (dynamicIndex == false)
				return new HttpResponseMessage(HttpStatusCode.BadRequest);

			var indexQuery = GetIndexQuery(Database.Configuration.MaxPageSize);
			string entityName = null;
			if (index.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase))
				entityName = index.Substring("dynamic/".Length);

			var explanations = Database.ExplainDynamicIndexSelection(entityName, indexQuery);

			return GetMessageWithObject(explanations);
		}

		private HttpResponseMessage GetIndexQueryResult(string index)
		{
			Etag indexEtag;
			var queryResult = ExecuteQuery(index, out indexEtag);

			if (queryResult == null)
				return null;

			var includes = GetQueryStringValues("include") ?? new string[0];

			var loadedIds = new HashSet<string>(
				queryResult.Results
					.Where(x => x["@metadata"] != null)
					.Select(x => x["@metadata"].Value<string>("@id"))
					.Where(x => x != null)
				);
			var command = new AddIncludesCommand(Database, GetRequestTransaction(),
												 (etag, doc) => queryResult.Includes.Add(doc), includes, loadedIds);
			foreach (var result in queryResult.Results)
			{
				command.Execute(result);
			}
			command.AlsoInclude(queryResult.IdsToInclude);

			if (queryResult.NonAuthoritativeInformation)
				return GetMessageWithString("", HttpStatusCode.NonAuthoritativeInformation, indexEtag);

			return GetMessageWithObject(queryResult, HttpStatusCode.OK, indexEtag);
		}

		private QueryResultWithIncludes ExecuteQuery(string index, out Etag indexEtag)
		{
			var indexQuery = GetIndexQuery(Database.Configuration.MaxPageSize);
			RewriteDateQueriesFromOldClients(indexQuery);

			var sp = Stopwatch.StartNew();
			var result = index.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) || index.Equals("dynamic", StringComparison.OrdinalIgnoreCase) ?
				PerformQueryAgainstDynamicIndex(index, indexQuery, out indexEtag) :
				PerformQueryAgainstExistingIndex(index, indexQuery, out indexEtag);

			sp.Stop();

			//context.Log(log => log.Debug(() =>
			//{
			//	var sb = new StringBuilder("\tQuery: ")
			//		.Append(indexQuery.Query)
			//		.AppendLine();
			//	sb.Append("\t").AppendFormat("Time: {0:#,#;;0} ms", sp.ElapsedMilliseconds).AppendLine();

			//	if (result == null)
			//		return sb.ToString();

			//	sb.Append("\tIndex: ")
			//		.AppendLine(result.IndexName);
			//	sb.Append("\t").AppendFormat("Results: {0:#,#;;0} returned out of {1:#,#;;0} total.", result.Results.Count, result.TotalResults).AppendLine();

			//	return sb.ToString();
			//}));

			return result;
		}

		private HttpResponseMessage GetIndexMappedResult(string index)
		{
			if (Database.IndexDefinitionStorage.GetIndexDefinition(index) == null)
				return new HttpResponseMessage(HttpStatusCode.NotFound);

			var key = GetQueryStringValue("key");
			if (string.IsNullOrEmpty(key))
			{
				List<string> keys = null;
				Database.TransactionalStorage.Batch(accessor =>
				{
					keys = accessor.MapReduce.GetKeysForIndexForDebug(index, GetStart(), GetPageSize(Database.Configuration.MaxPageSize))
						.ToList();
				});

				return GetMessageWithObject(new
				{
					Error = "Query string argument \'key\' is required",
					Keys = keys
				}, HttpStatusCode.BadRequest);
			}

			List<MappedResultInfo> mappedResult = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				mappedResult = accessor.MapReduce.GetMappedResultsForDebug(index, key, GetStart(), GetPageSize(Database.Configuration.MaxPageSize))
					.ToList();
			});

			return GetMessageWithObject(new
			{
				mappedResult.Count,
				Results = mappedResult
			});
		}

		private HttpResponseMessage GetIndexReducedResult(string index)
		{
			if (Database.IndexDefinitionStorage.GetIndexDefinition(index) == null)
				return new HttpResponseMessage(HttpStatusCode.NotFound);
			var key = GetQueryStringValue("key");
			if (string.IsNullOrEmpty(key))
				return GetMessageWithString("Query string argument 'key' is required", HttpStatusCode.BadRequest);

			int level;
			if (int.TryParse(GetQueryStringValue("level"), out level) == false || (level != 1 && level != 2))
				return GetMessageWithString("Query string argument 'level' is required and must be 1 or 2",
				                            HttpStatusCode.BadRequest);

			List<MappedResultInfo> mappedResult = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				mappedResult = accessor.MapReduce.GetReducedResultsForDebug(index, key, level, GetStart(), GetPageSize(Database.Configuration.MaxPageSize))
					.ToList();
			});

			return GetMessageWithObject(new
			{
				mappedResult.Count,
				Results = mappedResult
			});
		}

		private HttpResponseMessage GetIndexScheduledReduces(string index)
		{
			List<ScheduledReductionDebugInfo> mappedResult = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				mappedResult = accessor.MapReduce.GetScheduledReductionForDebug(index, GetStart(), GetPageSize(Database.Configuration.MaxPageSize))
					.ToList();
			});

			return GetMessageWithObject(new
			{
				mappedResult.Count,
				Results = mappedResult
			});
		}

		private HttpResponseMessage GetIndexKeysStats(string index)
		{
			if (Database.IndexDefinitionStorage.GetIndexDefinition(index) == null)
				return new HttpResponseMessage(HttpStatusCode.NotFound);

			List<ReduceKeyAndCount> keys = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				keys = accessor.MapReduce.GetKeysStats(index, GetStart(), GetPageSize(Database.Configuration.MaxPageSize))
					.ToList();
			});

			return GetMessageWithObject(new
			{
				keys.Count,
				Results = keys
			});
		}

		private HttpResponseMessage GetIndexStats(string index)
		{
			IndexStats stats = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				stats = accessor.Indexing.GetIndexStats(index);
			});

			if (stats == null)
				return new HttpResponseMessage(HttpStatusCode.NotFound);

			stats.LastQueryTimestamp = Database.IndexStorage.GetLastQueryTime(index);
			stats.Performance = Database.IndexStorage.GetIndexingPerformance(index);

			return GetMessageWithObject(stats);
		}

		private HttpResponseMessage GetIndexEntries(string index)
		{
			var indexQuery = GetIndexQuery(Database.Configuration.MaxPageSize);
			var totalResults = new Reference<int>();

			var isDynamic = index.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase)
							|| index.Equals("dynamic", StringComparison.OrdinalIgnoreCase);

			if (isDynamic)
				return GetMessageWithObject(GetIndexEntriesForDynamicIndex(index, indexQuery, totalResults));
			
			return GetMessageWithObject(GetIndexEntriesForExistingIndex(index, indexQuery, totalResults));
		}

		private HttpResponseMessage GetIndexEntriesForDynamicIndex(string index, IndexQuery indexQuery, Reference<int> totalResults)
		{
			string entityName;
			var dynamicIndexName = GetDynamicIndexName(index, indexQuery, out entityName);

			if (dynamicIndexName == null)
				return new HttpResponseMessage(HttpStatusCode.NotFound);

			return GetMessageWithObject(GetIndexEntriesForExistingIndex(dynamicIndexName, indexQuery, totalResults));
		}

		private HttpResponseMessage GetIndexEntriesForExistingIndex(string index, IndexQuery indexQuery, Reference<int> totalResults)
		{
			var results = Database
					.IndexStorage
					.IndexEntires(index, indexQuery, Database.IndexQueryTriggers, totalResults)
					.ToArray();

			Tuple<DateTime, Etag> indexTimestamp = null;
			var isIndexStale = false;

			Database.TransactionalStorage.Batch(
				accessor =>
				{
					isIndexStale = accessor.Staleness.IsIndexStale(index, indexQuery.Cutoff, indexQuery.CutoffEtag);
					if (isIndexStale == false && indexQuery.Cutoff == null && indexQuery.CutoffEtag == null)
					{
						var indexInstance = Database.IndexStorage.GetIndexInstance(index);
						isIndexStale = isIndexStale || (indexInstance != null && indexInstance.IsMapIndexingInProgress);
					}

					indexTimestamp = accessor.Staleness.IndexLastUpdatedAt(index);
				});
			var indexEtag = Database.GetIndexEtag(index, null, indexQuery.ResultsTransformer);

			return GetMessageWithObject(new
			{
				Count = results.Length,
				Results = results,
				Includes = new string[0],
				IndexTimestamp = indexTimestamp.Item1,
				IndexEtag = indexTimestamp.Item2,
				TotalResults = totalResults.Value,
				SkippedResults = 0,
				NonAuthoritativeInformation = false,
				ResultEtag = indexEtag,
				IsStale = isIndexStale,
				IndexName = index,
				LastQueryTime = Database.IndexStorage.GetLastQueryTime(index)
			}, HttpStatusCode.OK, indexEtag);
		}

		static readonly Regex OldDateTimeFormat = new Regex(@"(\:|\[|{|TO\s) \s* (\d{17})", RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);
		private void RewriteDateQueriesFromOldClients(IndexQuery indexQuery)
		{
			if (Request.Headers.Contains("Raven-Client-Version") == false)
				return;
			var clientVersion = Request.Headers.GetValues("Raven-Client-Version").FirstOrDefault();
			if (string.IsNullOrEmpty(clientVersion) == false) // new client
				return;

			var matches = OldDateTimeFormat.Matches(indexQuery.Query);
			if (matches.Count == 0)
				return;
			var builder = new StringBuilder(indexQuery.Query);
			for (var i = matches.Count - 1; i >= 0; i--) // working in reverse so as to avoid invalidating previous indexes
			{
				var dateTimeString = matches[i].Groups[2].Value;

				DateTime time;
				if (DateTime.TryParseExact(dateTimeString, "yyyyMMddHHmmssfff", CultureInfo.InvariantCulture, DateTimeStyles.None, out time) == false)
					continue;

				builder.Remove(matches[i].Groups[2].Index, matches[i].Groups[2].Length);
				var newDateTimeFormat = time.ToString(Default.DateTimeFormatsToWrite);
				builder.Insert(matches[i].Groups[2].Index, newDateTimeFormat);
			}
			indexQuery.Query = builder.ToString();
		}

		private QueryResultWithIncludes PerformQueryAgainstExistingIndex(string index, IndexQuery indexQuery, out Etag indexEtag)
		{
			indexEtag = Database.GetIndexEtag(index, null, indexQuery.ResultsTransformer);
			if (MatchEtag(indexEtag))
			{
				Database.IndexStorage.MarkCachedQuery(index);
				//context.SetStatusToNotModified();
				return null;
			}

			var queryResult = Database.Query(index, indexQuery);
			indexEtag = Database.GetIndexEtag(index, queryResult.ResultEtag, indexQuery.ResultsTransformer);
			return queryResult;
		}

		private QueryResultWithIncludes PerformQueryAgainstDynamicIndex(string index, IndexQuery indexQuery, out Etag indexEtag)
		{
			string entityName;
			var dynamicIndexName = GetDynamicIndexName(index, indexQuery, out entityName);

			if (dynamicIndexName != null && Database.IndexStorage.HasIndex(dynamicIndexName))
			{
				indexEtag = Database.GetIndexEtag(dynamicIndexName, null, indexQuery.ResultsTransformer);
				if (MatchEtag(indexEtag))
				{
					Database.IndexStorage.MarkCachedQuery(dynamicIndexName);
					//context.SetStatusToNotModified();
					return null;
				}
			}

			if (dynamicIndexName == null && // would have to create a dynamic index
				Database.Configuration.CreateAutoIndexesForAdHocQueriesIfNeeded == false) // but it is disabled
			{
				indexEtag = Etag.InvalidEtag;
				var explanations = Database.ExplainDynamicIndexSelection(entityName, indexQuery);
				//context.SetStatusToBadRequest();
				//var target = entityName == null ? "all documents" : entityName + " documents";
				//context.WriteJson(new
				//{
				//	Error = "Executing the query " + indexQuery.Query + " on " + target + " require creation of temporary index, and it has been explicitly disabled.",
				//	Explanations = explanations
				//});
				return null;
			}

			var queryResult = Database.ExecuteDynamicQuery(entityName, indexQuery);

			// have to check here because we might be getting the index etag just 
			// as we make a switch from temp to auto, and we need to refresh the etag
			// if that is the case. This can also happen when the optimizer
			// decided to switch indexes for a query.
			indexEtag = (dynamicIndexName == null || queryResult.IndexName == dynamicIndexName)
							? Database.GetIndexEtag(queryResult.IndexName, queryResult.ResultEtag, indexQuery.ResultsTransformer)
							: Etag.InvalidEtag;

			return queryResult;
		}

		private string GetDynamicIndexName(string index, IndexQuery indexQuery, out string entityName)
		{
			entityName = null;
			if (index.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase))
				entityName = index.Substring("dynamic/".Length);

			var dynamicIndexName = Database.FindDynamicIndexName(entityName, indexQuery);
			return dynamicIndexName;
		}
	}
}
