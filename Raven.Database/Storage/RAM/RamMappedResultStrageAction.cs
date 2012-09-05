using System;
using System.Collections.Generic;
using Raven.Abstractions;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Database.Storage.RAM
{
	class RamMappedResultStrageAction : IMappedResultsStorageAction
	{
		private readonly RamState state;
		private readonly IUuidGenerator generator;

		public RamMappedResultStrageAction(RamState state, IUuidGenerator generator)
		{
			this.state = state;
			this.generator = generator;
		}

		public void PutMappedResult(string view, string docId, string reduceKey, RavenJObject data)
		{
			var etag = generator.CreateSequentialUuid();
			var mapBucket = IndexingUtil.MapBucket(docId);

			var mappedResultToAdd = new MappedResultsWrapper
			{
				DocumentKey = docId,
				View = view,
				MappedResultInfo = new MappedResultInfo
				{
					Bucket = mapBucket,
					Data = data,
					ReduceKey = reduceKey,
					Timestamp = SystemTime.UtcNow,
					Etag = etag
				}
			};

			var mappedResult = state.MappedResults.GetOrAdd(view);

			mappedResult.Add(mappedResultToAdd);
		}

		public void DeleteMappedResultsForDocumentId(string documentId, string view, HashSet<ReduceKeyAndBucket> removed)
		{
			var mappedResult = state.MappedResults.GetOrDefault(view);
			if (mappedResult == null)
				return;

			var docsToDelete = mappedResult.Where(wrapper => wrapper.DocumentKey == documentId).ToList();
			if (docsToDelete.Count == 0)
				return;

			foreach (var mappedResultsWrapper in docsToDelete)
			{

				var reduceKey = mappedResultsWrapper.MappedResultInfo.ReduceKey;
				var bucket = mappedResultsWrapper.MappedResultInfo.Bucket;

				removed.Add(new ReduceKeyAndBucket(bucket, reduceKey));
				
				state.MappedResults.GetOrDefault(view).Remove(mappedResultsWrapper);
			}
		}

		public void DeleteMappedResultsForView(string view)
		{
			var mappedResults = state.MappedResults.GetOrDefault(view);

			if (mappedResults == null)
				return;

			state.MappedResults.Remove(view);
			//Api.MakeKey(session, MappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			//Api.JetSetIndexRange(session, MappedResults, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);

			//do
			//{
			//	// esent index ranges are approximate, and we need to check them ourselves as well
			//	var viewFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"]);
			//	if (StringComparer.InvariantCultureIgnoreCase.Equals(viewFromDb, view) == false)
			//		continue;
			//	Api.JetDelete(session, MappedResults);
			//} while (Api.TryMoveNext(session, MappedResults));
		}

		public IEnumerable<MappedResultInfo> GetMappedResultsReduceKeysAfter(string indexName, Guid lastReducedEtag, bool loadData, int take)
		{
			var mappedResults =
				state.MappedResults.GetOrDefault(indexName).Where(wrapper => wrapper.MappedResultInfo.Etag == lastReducedEtag).Take(take);
			
			if (!mappedResults.Any())
				return Enumerable.Empty<MappedResultInfo>();

			var results = new Dictionary<string, MappedResultInfo>();

			foreach (var mappedResult in results)
			{
				var key = mappedResult.Value.ReduceKey;
				var mappedResultInfo = new MappedResultInfo
				{
					ReduceKey = key,
					Etag = mappedResult.Value.Etag,
					Timestamp = mappedResult.Value.Timestamp,
					Data = loadData ? LoadMappedResults(key) : null,
					Size = mappedResult.Value.Size
				};

				results[mappedResultInfo.ReduceKey] = mappedResultInfo;
			}

			//while (
			//	results.Count < take &&
			//	Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"], Encoding.Unicode, RetrieveColumnGrbit.RetrieveFromIndex) == indexName)
			//{
			//	var key = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"]);
			//	var mappedResultInfo = new MappedResultInfo
			//	{
			//		ReduceKey =
			//			key,
			//		Etag = new Guid(Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"])),
			//		Timestamp =
			//			Api.RetrieveColumnAsDateTime(session, MappedResults, tableColumnsCache.MappedResultsColumns["timestamp"]).
			//			Value,
			//		Data = loadData
			//				? LoadMappedResults(key)
			//				: null,
			//		Size = Api.RetrieveColumnSize(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"]) ?? 0
			//	};

			//	results[mappedResultInfo.ReduceKey] = mappedResultInfo;

			//	// the index is view ascending and etag descending
			//	// that means that we are going backward to go up
			//	if (Api.TryMovePrevious(session, MappedResults) == false)
			//		break;
			//}

			return results.Values;
		}

		public IEnumerable<MappedResultInfo> GetMappedResultsForDebug(string indexName, string key, int take)
		{
			var results = state.MappedResults.GetOrDefault(indexName).Where(wrapper => wrapper.MappedResultInfo.ReduceKey == key).Take(take);

			if (!results.Any())
				yield break;

			foreach (var mappedResultsWrapper in results)
			{
				var bucket = mappedResultsWrapper.MappedResultInfo.Bucket;
				yield return new MappedResultInfo
				{
					ReduceKey = key,
					Etag = mappedResultsWrapper.MappedResultInfo.Etag,
					Timestamp = mappedResultsWrapper.MappedResultInfo.Timestamp,
					Data = LoadMappedResults(key),
					Size = mappedResultsWrapper.MappedResultInfo.Size,
					Bucket = bucket,
					Source = mappedResultsWrapper.DocumentKey
				};
			}

			//while (take > 0)
			//{
			//	take -= 1;

			//	var indexNameFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"], Encoding.Unicode, RetrieveColumnGrbit.RetrieveFromIndex);
			//	var keyFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"]);
			//	if (string.Equals(indexNameFromDb, indexName, StringComparison.InvariantCultureIgnoreCase) == false ||
			//		string.Equals(key, keyFromDb, StringComparison.InvariantCultureIgnoreCase) == false)
			//		break;

			//	var bucket = Api.RetrieveColumnAsInt32(session, MappedResults, tableColumnsCache.MappedResultsColumns["bucket"]).Value;
			//	yield return new MappedResultInfo
			//	{
			//		ReduceKey = keyFromDb,
			//		Etag = new Guid(Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"])),
			//		Timestamp = Api.RetrieveColumnAsDateTime(session, MappedResults, tableColumnsCache.MappedResultsColumns["timestamp"]).Value,
			//		Data = LoadMappedResults(keyFromDb),
			//		Size = Api.RetrieveColumnSize(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"]) ?? 0,
			//		Bucket = bucket,
			//		Source = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["document_key"], Encoding.Unicode)
			//	};

			//	if (Api.TryMoveNext(session, MappedResults) == false)
			//		break;
			//}
		}

		public IEnumerable<MappedResultInfo> GetReducedResultsForDebug(string indexName, string key, int level, int take)
		{
			throw new NotImplementedException();
		}

		public void ScheduleReductions(string view, int level, IEnumerable<ReduceKeyAndBucket> reduceKeysAndBuckets)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<MappedResultInfo> GetItemsToReduce(string index, int level, int take, List<object> itemsToDelete)
		{
			throw new NotImplementedException();
		}

		public ScheduledReductionInfo DeleteScheduledReduction(List<object> itemsToDelete)
		{
			throw new NotImplementedException();
		}

		public void PutReducedResult(string view, string reduceKey, int level, int sourceBucket, int bucket, RavenJObject data)
		{
			var etag = generator.CreateSequentialUuid();

			var result = state.MappedResults.GetOrAdd(view);

			result.Add(new MappedResultsWrapper
			{
				View = view,
				MappedResultInfo = new MappedResultInfo
				{
					Bucket = bucket,
					Etag = etag,
					ReduceKey = reduceKey,
					Data = data,
					Timestamp = SystemTime.UtcNow
				},
				Level = level,
				SourceBucket = sourceBucket
			});
		}

		public void RemoveReduceResults(string indexName, int level, string reduceKey, int sourceBucket)
		{
			var reducedResults = state.MappedResults.Select(pair => pair.Value.Where
				                                                        (wrapper => wrapper.Level == level &&
				                                                                    wrapper.SourceBucket == sourceBucket &&
				                                                                    wrapper.MappedResultInfo.ReduceKey == reduceKey)).ToList();

			if (reducedResults.Count == 0)
				return;

			foreach (var reducedResult in reducedResults)
			{
				//delete
			}
			Api.JetSetCurrentIndex(session, ReducedResults, "by_view_level_reduce_key_and_source_bucket");
			Api.MakeKey(session, ReducedResults, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, ReducedResults, level, MakeKeyGrbit.None);
			Api.MakeKey(session, ReducedResults, reduceKey, Encoding.Unicode, MakeKeyGrbit.None);
			Api.MakeKey(session, ReducedResults, sourceBucket, MakeKeyGrbit.None);

			if (Api.TrySeek(session, ReducedResults, SeekGrbit.SeekEQ) == false)
				return;

			Api.MakeKey(session, ReducedResults, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, ReducedResults, level, MakeKeyGrbit.None);
			Api.MakeKey(session, ReducedResults, reduceKey, Encoding.Unicode, MakeKeyGrbit.None);
			Api.MakeKey(session, ReducedResults, sourceBucket, MakeKeyGrbit.None);
			Api.JetSetIndexRange(session, ReducedResults, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);

			do
			{
				Api.JetDelete(session, ReducedResults);
			} while (Api.TryMoveNext(session, ReducedResults));
		}
	}
}