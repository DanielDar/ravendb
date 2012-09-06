using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Indexing;

namespace Raven.Database.Storage.RAM
{
	class RamIndexingStorageActions : IIndexingStorageActions
	{
		private readonly RamState state;

		public RamIndexingStorageActions(RamState state)
		{
			this.state = state;
		}

		public void Dispose()
		{
			
		}

		public IEnumerable<IndexStats> GetIndexesStats()
		{
			return state.IndexesStats.Select(pair => pair.Value);
		}

		public void AddIndex(string name, bool createMapReduce)
		{
			state.IndexesStats.Set(name, new IndexStats
			{
				Name = name,
				LastIndexedEtag = Guid.Empty,
				LastIndexedTimestamp = DateTime.MinValue,
			});

			state.IndexesEtag.Set(name, 0);

			if (createMapReduce == false)
				return;

			state.IndexesReduceStats.Set(name, new IndexStats
			{
				Name = name,
				LastIndexedEtag = Guid.Empty,
				LastIndexedTimestamp = DateTime.MinValue
			});
		}

		public void DeleteIndex(string name)
		{
			var indexStat = state.IndexesStats.GetOrDefault(name);
			if (indexStat != null)
				state.IndexesStats.Remove(name);

			var indexEtag = state.IndexesEtag.GetOrDefault(name);
			if (indexEtag != 0)
				state.IndexesEtag.Remove(name);

			var indexReduceStat = state.IndexesReduceStats.GetOrDefault(name);
			if (indexReduceStat != null)
				state.IndexesReduceStats.Remove(name);

			state.MappedResults.Remove(name);
			state.ReducedResults.Remove(name);
			state.ScheduledReductions.Remove(name);
		}

		public IndexFailureInformation GetFailureRate(string index)
		{
			var hasReduce = SetCurrentIndexStatsToImpl(index);
			return new IndexFailureInformation
			{
				Name = index,
				Attempts = state.IndexesStats.GetOrDefault(index).IndexingAttempts,
				Errors = state.IndexesStats.GetOrDefault(index).IndexingErrors,
				Successes = state.IndexesStats.GetOrDefault(index).IndexingSuccesses,
				ReduceAttempts = hasReduce ? state.IndexesReduceStats.GetOrDefault(index).ReduceIndexingAttempts : null,
				ReduceErrors = hasReduce ? state.IndexesReduceStats.GetOrDefault(index).ReduceIndexingErrors : null,
				ReduceSuccesses = hasReduce ? state.IndexesReduceStats.GetOrDefault(index).ReduceIndexingSuccesses : null
			};
		}

		public void UpdateLastIndexed(string index, Guid etag, DateTime timestamp)
		{
			var indexstat = state.IndexesStats.GetOrDefault(index);

			if (indexstat == null)
				throw new IndexDoesNotExistsException("There is no index named: " + index);

			indexstat.LastIndexedEtag = etag;
			indexstat.LastIndexedTimestamp = timestamp;
		}

		public void UpdateLastReduced(string index, Guid etag, DateTime timestamp)
		{
			var indexReduceStat = state.IndexesReduceStats.GetOrDefault(index);

			if (indexReduceStat == null)
				throw new IndexDoesNotExistsException("There is no reduce index named: " + index);

			indexReduceStat.LastIndexedEtag = etag;
			indexReduceStat.LastIndexedTimestamp = timestamp;
		}

		public void TouchIndexEtag(string index)
		{
			if (state.IndexesEtag.Any(pair => pair.Key == index) == false)
				throw new IndexDoesNotExistsException("There is no reduce index named: " + index);

			var touch = state.IndexesEtag.GetOrDefault(index);

			state.IndexesEtag.Set(index, touch + 1);
		}

		public void UpdateIndexingStats(string index, IndexingWorkStats stats)
		{
			SetCurrentIndexStatsToImpl(index);

			var indexStat = state.IndexesStats.GetOrDefault(index);

			state.IndexesStats.Set(index, new IndexStats
			{
				Name = indexStat.Name,
				IndexingAttempts = indexStat.IndexingAttempts + stats.IndexingAttempts,
				IndexingErrors = indexStat.IndexingErrors + stats.IndexingErrors,
				IndexingSuccesses = indexStat.IndexingSuccesses + stats.IndexingSuccesses,
				LastIndexedEtag = indexStat.LastIndexedEtag,
				LastIndexedTimestamp = indexStat.LastIndexedTimestamp,
				LastQueryTimestamp = indexStat.LastQueryTimestamp,
				LastReducedEtag = indexStat.LastReducedEtag,
				LastReducedTimestamp = indexStat.LastReducedTimestamp,
				ReduceIndexingAttempts = indexStat.ReduceIndexingAttempts,
				ReduceIndexingErrors = indexStat.ReduceIndexingErrors,
				ReduceIndexingSuccesses = indexStat.ReduceIndexingSuccesses,
				TouchCount = indexStat.TouchCount
			});
		}

		public void UpdateReduceStats(string index, IndexingWorkStats stats)
		{
			SetCurrentIndexStatsToImpl(index);

			var reduceStat = state.IndexesReduceStats.GetOrDefault(index);

			if(reduceStat == null)
				return;

			state.IndexesStats.Set(index, new IndexStats
			{
				Name = reduceStat.Name,
				IndexingAttempts = reduceStat.IndexingAttempts,
				IndexingErrors = reduceStat.IndexingErrors,
				IndexingSuccesses = reduceStat.IndexingSuccesses,
				LastIndexedEtag = reduceStat.LastIndexedEtag,
				LastIndexedTimestamp = reduceStat.LastIndexedTimestamp,
				LastQueryTimestamp = reduceStat.LastQueryTimestamp,
				LastReducedEtag = reduceStat.LastReducedEtag,
				LastReducedTimestamp = reduceStat.LastReducedTimestamp,
				ReduceIndexingAttempts = reduceStat.ReduceIndexingAttempts + stats.ReduceAttempts,
				ReduceIndexingErrors = reduceStat.ReduceIndexingErrors + stats.ReduceErrors,
				ReduceIndexingSuccesses = reduceStat.ReduceIndexingSuccesses + stats.ReduceSuccesses,
				TouchCount = reduceStat.TouchCount
			});
		}

		private bool SetCurrentIndexStatsToImpl(string index)
		{
			var indexStats = state.IndexesStats.GetOrDefault(index);
			if (indexStats == null)
				throw new IndexDoesNotExistsException("There is no index named: " + index);

			// this is optional
			var reduceStat = state.IndexesReduceStats.GetOrDefault(index);
			return reduceStat != null;
		}
	}
}
