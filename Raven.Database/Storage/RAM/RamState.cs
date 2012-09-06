using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Database.Storage.RAM
{
	public class RamState
	{
		public TransactionalDictionary<string, TransactionalDictionary<string, ListItem>> Lists { get; private set; }
		public TransactionalDictionary<string, TransactionalDictionary<Guid, byte[]>> Queues { get; private set; }
		public TransactionalDictionary<string, TransactionalDictionary<Guid, TaskWrapper>> Tasks { get; private set; }
		public TransactionalDictionary<string, TransactionalValue<long>> Identities { get; private set; }
		public TransactionalDictionary<string, Attachment> Attachments { get; private set; }
		public TransactionalValue<int> AttachmentCount { get; private set; }
		public TransactionalDictionary<string, DocuementWrapper> Documents { get; private set; }
		public TransactionalDictionary<string, DocumentsModifiedByTransation> DocumentsModifiedByTransations { get; set; }
		public TransactionalValue<long> DocumentCount { get; private set; }
		public TransactionalDictionary<Guid, Transaction> Transactions { get; private set; }
		public TransactionalDictionary<string, IndexStats> IndexesStats { get; private set; }
		public TransactionalDictionary<string, IndexStats> IndexesReduceStats { get; private set; }
		public TransactionalDictionary<string, TransactionalList<MappedResultsWrapper>> MappedResults { get; private set; }
		public TransactionalDictionary<string, TransactionalList<ReducedResultsWrapper>> ReducedResults { get; private set; }
		public TransactionalDictionary<string, TransactionalList<ScheduledReductionsWrapper>> ScheduledReductions { get; private set; }
		public TransactionalDictionary<string, int> IndexesEtag { get; private set; } 

		public RamState()
		{
			AttachmentCount = new TransactionalValue<int>();

			Attachments = new TransactionalDictionary<string, Attachment>(StringComparer.InvariantCultureIgnoreCase);

			DocumentCount = new TransactionalValue<long>();

			Documents = new  TransactionalDictionary<string, DocuementWrapper>(StringComparer.InvariantCultureIgnoreCase);

			Transactions = new TransactionalDictionary<Guid, Transaction>(EqualityComparer<Guid>.Default);

			IndexesStats = new TransactionalDictionary<string, IndexStats>(StringComparer.InvariantCultureIgnoreCase);

			IndexesReduceStats = new TransactionalDictionary<string, IndexStats>(StringComparer.InvariantCultureIgnoreCase);

			IndexesEtag = new TransactionalDictionary<string, int>(StringComparer.InvariantCultureIgnoreCase);

			MappedResults = new TransactionalDictionary<string, TransactionalList<MappedResultsWrapper>>(StringComparer.InvariantCultureIgnoreCase,
				() => new TransactionalList<MappedResultsWrapper>());

			ReducedResults = new TransactionalDictionary<string, TransactionalList<ReducedResultsWrapper>>(StringComparer.InvariantCultureIgnoreCase,
				() => new TransactionalList<ReducedResultsWrapper>());

			ScheduledReductions = new TransactionalDictionary<string, TransactionalList<ScheduledReductionsWrapper>>(StringComparer.InvariantCultureIgnoreCase,
				() => new TransactionalList<ScheduledReductionsWrapper>());

			Lists = new TransactionalDictionary<string, TransactionalDictionary<string, ListItem>>(StringComparer.InvariantCultureIgnoreCase,
					() => new TransactionalDictionary<string, ListItem>(StringComparer.InvariantCultureIgnoreCase));

			Queues = new TransactionalDictionary<string, TransactionalDictionary<Guid, byte[]>>(StringComparer.InvariantCultureIgnoreCase,
				() => new TransactionalDictionary<Guid, byte[]>(EqualityComparer<Guid>.Default));

			Tasks = new TransactionalDictionary<string, TransactionalDictionary<Guid, TaskWrapper>>(StringComparer.InvariantCultureIgnoreCase,
				() => new TransactionalDictionary<Guid, TaskWrapper>(EqualityComparer<Guid>.Default));

			Identities = new TransactionalDictionary<string, TransactionalValue<long>>(StringComparer.InvariantCultureIgnoreCase,
				() => new TransactionalValue<long>{Value = 0L});
		}
	}

	public class Transaction
	{
		public Guid Key { get; set; }
		public DateTime TimeOut { get; set; }
	}

	public class DocuementWrapper
	{
		public JsonDocument Document { get; set; }
		public Guid? LockByTransaction { get; set; }
	}

	public class DocumentsModifiedByTransation
	{
		public JsonDocument Document { get; set; }
		public Guid? LockByTransaction { get; set; }
		public bool DeleteDocument { get; set; }
	}

	public class MappedResultsWrapper
	{
		public MappedResultInfo MappedResultInfo { get; set; }
		public string View { get; set; }
		public string DocumentKey { get; set; }
		public int Level { get; set; }
		public string SourceBucket { get; set; }
	}

	public class ReducedResultsWrapper
	{
		public MappedResultInfo MappedResultInfo { get; set; }
		public string View { get; set; }
		public int Level { get; set; }
		public string SourceBucket { get; set; }
	}

	public class ScheduledReductionsWrapper
	{
		public ScheduledReductionInfo ScheduledReductionInfo { get; set; }
		public int Level { get; set; }
		public string View { get; set; }
		public string ReduceKey { get; set; }
		public int SourceBucket { get; set; }

	}

	public class TaskWrapper
	{
		public byte[] Task { get; set; }
		public DateTime AddedAt { get; set; }
	}
}