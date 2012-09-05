using System;
using System.Linq;
using Raven.Database.Exceptions;

namespace Raven.Database.Storage.RAM
{
	public class RamStalenessStorageActions : IStalenessStorageActions
	{
		private readonly RamState state;

		public RamStalenessStorageActions(RamState state)
		{
			this.state = state;
		}

		public bool IsIndexStale(string name, DateTime? cutOff, Guid? cutoffEtag)
		{
			var indexStat = state.IndexesStats.GetOrDefault(name);

			if (indexStat == null)
				return false;

			var indexStatReduce = state.IndexesReduceStats.GetOrDefault(name);

			var hasReduce = indexStatReduce != null;

			if (IsMapStale(name) || hasReduce && IsReduceStale(name))
			{
				if (cutOff != null)
				{
					var lastIndexedTimestamp = indexStat.LastIndexedTimestamp;
					if (cutOff.Value >= lastIndexedTimestamp)
						return true;

					if (hasReduce)
					{
						lastIndexedTimestamp = indexStatReduce.LastIndexedTimestamp;
						if (cutOff.Value >= lastIndexedTimestamp)
							return true;
					}
				}
				else if (cutoffEtag != null)
				{
					var lastIndexedEtag = indexStat.LastIndexedEtag;

					if (lastIndexedEtag.CompareTo(cutoffEtag) < 0)
						return true;
				}
				else
				{
					return true;
				}
			}

			var task = state.Tasks.GetOrDefault(name);

			if (task == null)
				return false;

			var lastTask = task.OrderByDescending(pair => pair.Key).FirstOrDefault();

			if (lastTask.Value == null)
				return false;
			
			if (cutOff == null)
				return true;

			// we are at the first row for this index

			var addedAt = lastTask.Value.AddedAt;
			return cutOff.Value >= addedAt;
		}

		public bool IsReduceStale(string name)
		{
			return state.ScheduledReductions.Any(pair => pair.Key == name);
		}

		public bool IsMapStale(string name)
		{
			var indexStat = state.IndexesStats.GetOrDefault(name);

			if (indexStat == null)
				return false;

			var lastIndexedEtag = indexStat.LastIndexedEtag;
		
			var lastEtag = (Guid)state.Documents.OrderByDescending(pair => pair.Value.Document.Etag).Select(pair => pair.Value.Document.Etag).FirstOrDefault();
			return lastEtag.CompareTo(lastIndexedEtag) > 0;
		}

		public Tuple<DateTime, Guid> IndexLastUpdatedAt(string name)
		{
			var indexStat = state.IndexesStats.GetOrDefault(name);

			if (indexStat == null)
				throw new IndexDoesNotExistsException("Could not find index named: " + name);

			var indexReduceStat = state.IndexesReduceStats.GetOrDefault(name);

			if (indexReduceStat != null)
			{// for map-reduce indexes, we use the reduce stats

				var lastReducedIndex = (DateTime) indexReduceStat.LastReducedTimestamp;
				var lastReducedEtag = (Guid)indexReduceStat.LastReducedEtag;
				return Tuple.Create(lastReducedIndex, lastReducedEtag);
			}

			var lastIndexedTimestamp = indexStat.LastIndexedTimestamp;
			var lastIndexedEtag = indexStat.LastIndexedEtag;

			return Tuple.Create(lastIndexedTimestamp, lastIndexedEtag);
		}

		public Guid GetMostRecentDocumentEtag()
		{
			var lastEtag = state.Documents
				.OrderByDescending(pair => pair.Value.Document.Etag)
				.Select(pair => pair.Value.Document.Etag)
				.FirstOrDefault();

			if (lastEtag == null)
				return Guid.Empty;

			return (Guid)lastEtag;
		}

		public Guid GetMostRecentAttachmentEtag()
		{
			var lastEtag = state.Attachments
				.OrderByDescending(pair => pair.Value.Etag)
				.Select(pair => pair.Value.Etag)
				.FirstOrDefault();

			return lastEtag;
		}

		public Guid? GetMostRecentReducedEtag(string name)
		{
			throw new NotImplementedException();
		}

		public int GetIndexTouchCount(string indexName)
		{
			var indexStat = state.IndexesStats.GetOrDefault(indexName);
			if (indexStat == null)
				return -1;

			return indexStat.TouchCount;
		}
	}
}
