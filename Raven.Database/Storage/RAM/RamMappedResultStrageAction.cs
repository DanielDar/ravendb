using System;
using System.Collections.Generic;
using System.Globalization;
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
					Data = loadData ? mappedResult.Value.Data : null,
					Size = mappedResult.Value.Size
				};

				results[mappedResultInfo.ReduceKey] = mappedResultInfo;
			}

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
					Data = mappedResultsWrapper.MappedResultInfo.Data,
					Size = mappedResultsWrapper.MappedResultInfo.Size,
					Bucket = bucket,
					Source = mappedResultsWrapper.DocumentKey
				};
			}
		}

		public IEnumerable<MappedResultInfo> GetReducedResultsForDebug(string indexName, string key, int level, int take)
		{
			var reducedResults = state.ReducedResults.GetOrDefault(indexName);
			if(reducedResults == null)
				yield break;

			var resultsToDebug =
				reducedResults.Where(wrapper => wrapper.MappedResultInfo.ReduceKey == key && wrapper.Level == level).Take(take).ToList();

			foreach (var reducedResultsWrapper in resultsToDebug)
			{
				var levelFromDb = reducedResultsWrapper.Level;
				var indexNameFromDb = reducedResultsWrapper.View;
				var keyFromDb = reducedResultsWrapper.MappedResultInfo.ReduceKey;

				if (string.Equals(indexNameFromDb, indexName, StringComparison.InvariantCultureIgnoreCase) == false ||
					level != levelFromDb ||
					string.Equals(key, keyFromDb, StringComparison.InvariantCultureIgnoreCase) == false)
					break;

				yield return new MappedResultInfo
				{
					ReduceKey = keyFromDb,
					Etag = reducedResultsWrapper.MappedResultInfo.Etag,
					Timestamp = reducedResultsWrapper.MappedResultInfo.Timestamp,
					Data = reducedResultsWrapper.MappedResultInfo.Data,
					Size = reducedResultsWrapper.MappedResultInfo.Size,
					Bucket = reducedResultsWrapper.MappedResultInfo.Bucket,
					Source = reducedResultsWrapper.SourceBucket
				};
			}
		}

		public void ScheduleReductions(string view, int level, IEnumerable<ReduceKeyAndBucket> reduceKeysAndBuckets)
		{
			foreach (var reduceKeysAndBukcet in reduceKeysAndBuckets)
			{
				var bucket = reduceKeysAndBukcet.Bucket;

				var scheduledReductions = state.ScheduledReductions.GetOrAdd(view);

				scheduledReductions.Add(new ScheduledReductionsWrapper
				{
					Level = level,
					View = view,
					ReduceKey = reduceKeysAndBukcet.ReduceKey,
					SourceBucket = bucket,
					ScheduledReductionInfo = new ScheduledReductionInfo
					{
						Etag = generator.CreateSequentialUuid(),
						Timestamp = SystemTime.UtcNow
					}
				});
			}
		}

		public IEnumerable<MappedResultInfo> GetItemsToReduce(string index, int level, int take, List<object> itemsToDelete)
		{
			var scheduledReductions = state.ScheduledReductions.GetOrDefault(index);
			if (scheduledReductions == null)
				yield break;

			var scheduledReductionsToReduce = scheduledReductions.Where(wrapper => wrapper.Level == level).Take(take).ToList();

			var seen = new HashSet<Tuple<string, int>>();

			foreach (var reductionsWrapper in scheduledReductionsToReduce)
			{
				var indexFromDb = reductionsWrapper.View;
				var levelFromDb = reductionsWrapper.Level;

				if (string.Equals(index, indexFromDb, StringComparison.InvariantCultureIgnoreCase) == false ||
					levelFromDb != level)
					break;

				var reduceKey = reductionsWrapper.ReduceKey;
				var bucket = reductionsWrapper.SourceBucket;

				if (seen.Add(Tuple.Create(reduceKey, bucket)))
				{
					foreach (var mappedResultInfo in GetResultsForBucket(index, level, reduceKey, bucket))
					{
						take--;
						yield return mappedResultInfo;
					}
				}
			}
		}

		public ScheduledReductionInfo DeleteScheduledReduction(List<object> itemsToDelete)
		{
			//TODO: check
			foreach (var item in itemsToDelete)
			{
				state.ScheduledReductions.Remove(item.ToString());

			}

			return null;
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
				SourceBucket = sourceBucket.ToString(CultureInfo.InvariantCulture)
			});
		}

		public void RemoveReduceResults(string indexName, int level, string reduceKey, int sourceBucket)
		{
			var reducedResults = state.ReducedResults.GetOrDefault(indexName);
			if (reducedResults == null)
				return;
			var resultsToRemove =
				reducedResults.Where(
					wrapper =>
					wrapper.Level == level && wrapper.MappedResultInfo.ReduceKey == reduceKey && wrapper.SourceBucket == sourceBucket.ToString(CultureInfo.InvariantCulture)).ToList();

			if (!resultsToRemove.Any())
				return;

			foreach (var reducedResultsWrapper in resultsToRemove)
			{
				state.ReducedResults.GetOrDefault(indexName).Remove(reducedResultsWrapper);
			}

			if(!state.ReducedResults.GetOrDefault(indexName).Any())
			{
				state.ReducedResults.Remove(indexName);
			}
		}

		private IEnumerable<MappedResultInfo> GetResultsForBucket(string index, int level, string reduceKey, int bucket)
		{
			switch (level)
			{
				case 0:
					return GetMappedResultsForBucket(index, reduceKey, bucket);
				case 1:
				case 2:
					return GetReducedResultsForBucket(index, reduceKey, level, bucket);
				default:
					throw new ArgumentException("Invalid level: " + level);
			}
		}

		private IEnumerable<MappedResultInfo> GetMappedResultsForBucket(string index, string reduceKey, int bucket)
		{
			var mappedResults = state.MappedResults.GetOrDefault(index);
			if(mappedResults == null)
			{
				yield return new MappedResultInfo
				{
					ReduceKey = reduceKey,
					Bucket = bucket
				};
				yield break;
			}

			var mappedResultsForBucket =
				mappedResults.Where(wrapper => wrapper.MappedResultInfo.ReduceKey == reduceKey
				                               && wrapper.MappedResultInfo.Bucket == bucket)
					.ToList();

			if (mappedResultsForBucket.Count == 0)
			{
				yield return new MappedResultInfo
				{
					ReduceKey = reduceKey,
					Bucket = bucket
				};
				yield break;
			}

			foreach (var mappedResultsWrapper in mappedResultsForBucket)
			{
				var indexFromDb = mappedResultsWrapper.View;
				var keyFromDb = mappedResultsWrapper.MappedResultInfo.ReduceKey;
				var bucketFromDb = mappedResultsWrapper.MappedResultInfo.Bucket;

				if (string.Equals(indexFromDb, index, StringComparison.InvariantCultureIgnoreCase) == false ||
					string.Equals(keyFromDb, reduceKey, StringComparison.InvariantCultureIgnoreCase) == false ||
					bucketFromDb != bucket)
					break;

				yield return new MappedResultInfo
				{
					Bucket = bucket,
					ReduceKey = keyFromDb,
					Etag = mappedResultsWrapper.MappedResultInfo.Etag,
					Timestamp = mappedResultsWrapper.MappedResultInfo.Timestamp,
					Data = mappedResultsWrapper.MappedResultInfo.Data,
					Size = mappedResultsWrapper.MappedResultInfo.Size
				};
			}
		}

		private IEnumerable<MappedResultInfo> GetReducedResultsForBucket(string index, string reduceKey, int level, int bucket)
		{
			var reducedResults = state.ReducedResults.GetOrDefault(index);
			if(reducedResults == null)
			{
				yield return new MappedResultInfo
				{
					Bucket = bucket,
					ReduceKey = reduceKey,
				};
				yield break;
			}

			var reduceResultsForBucket = reducedResults.Where(wrapper => wrapper.Level == level
			                                                             && wrapper.View == index
			                                                             && wrapper.MappedResultInfo.ReduceKey == reduceKey
			                                                             && wrapper.MappedResultInfo.Bucket == bucket)
				.ToList();

			if (reduceResultsForBucket.Count == 0)
			{
				yield return new MappedResultInfo
				{
					Bucket = bucket,
					ReduceKey = reduceKey,
				};
				yield break;
			}


			foreach (var reducedResultsWrapper in reduceResultsForBucket)
			{
				var key = reducedResultsWrapper.MappedResultInfo.ReduceKey;
				var bucketFromDb = reducedResultsWrapper.MappedResultInfo.Bucket;
				if (string.Equals(key, reduceKey, StringComparison.InvariantCultureIgnoreCase) == false ||
					bucketFromDb != bucket)
					break;

				yield return new MappedResultInfo
				{
					Bucket = bucket,
					ReduceKey = key,
					Etag = reducedResultsWrapper.MappedResultInfo.Etag,
					Timestamp = reducedResultsWrapper.MappedResultInfo.Timestamp,
					Data = reducedResultsWrapper.MappedResultInfo.Data,
					Size = reducedResultsWrapper.MappedResultInfo.Size
				};
			}
		}
	}
}