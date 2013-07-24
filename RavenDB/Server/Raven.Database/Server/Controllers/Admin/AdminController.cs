using System;
using System.Net;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using Raven.Database.Data;
using System.Net.Http;

namespace Raven.Database.Server.Controllers.Admin
{
	[RoutePrefix("admin")]
	public class AdminController : BaseAdminController
	{
		[HttpPost("backup")]
		public async Task<object> Backup()
		{
			var backupRequest = await ReadJsonObjectAsync<BackupRequest>();
			var incrementalString = Request.RequestUri.ParseQueryString()["incremental"];
			bool incrementalBackup;
			if (bool.TryParse(incrementalString, out incrementalBackup) == false)
				incrementalBackup = false;
			Database.StartBackup(backupRequest.BackupLocation, incrementalBackup, backupRequest.DatabaseDocument);

			return new HttpResponseMessage(HttpStatusCode.Created);		
		}

		[HttpGet("changedbid")]
		public object ChangeDbId()
		{
			Guid old = Database.TransactionalStorage.Id;
			var newId = Database.TransactionalStorage.ChangeId();

			return new
			{
				OldId = old,
				NewId = newId
			};
		}

		[HttpGet("compact")]
		public void Compact()
		{
			EnsureSystemDatabase();
				
			var db = Request.RequestUri.ParseQueryString()["database"];
			if(string.IsNullOrWhiteSpace(db))
				throw new HttpException(400, "Compact request requires a valid database parameter");

			var configuration = DatabasesLandlord.CreateTenantConfiguration(db);
			if (configuration == null)
				throw new HttpException(404, "No database named: " + db);

			DatabasesLandlord.LockDatabase(db, () => DatabasesLandlord.SystemDatabase.TransactionalStorage.Compact(configuration));
		}

		[HttpGet("indexingStatus")]
		public object IndexingStatus()
		{
			return new { IndexingStatus = Database.WorkContext.RunIndexing ? "Indexing" : "Paused" };			
		}

		[HttpGet("optimize")]
		public void Optimize()
		{
			Database.IndexStorage.MergeAllIndexes();			
		}

		[HttpGet("startIndexing")]
		public void StartIndexing()
		{
			var concurrency = Request.RequestUri.ParseQueryString()["concurrency"];

			if (string.IsNullOrEmpty(concurrency) == false)
			{
				Database.Configuration.MaxNumberOfParallelIndexTasks = Math.Max(1, int.Parse(concurrency));
			}

			Database.SpinIndexingWorkers();
		}

		[HttpGet("stopIndexing")]
		public void StopIndexing()
		{
			Database.StopIndexingWorkers();			
		}

		[HttpGet("stats")]
		public object Stats()
		{
			if (Database != DatabasesLandlord.SystemDatabase)
				throw new HttpException(404, "Admin stats can only be had from the root database");

			return DatabasesLandlord.SystemDatabase.Statistics;
		}

		[HttpGet("gc")]
		[HttpPost("gc")]
		public void Gc()
		{
			EnsureSystemDatabase();
			CollectGarbage(Database);
		}

		public static void CollectGarbage(DocumentDatabase database)
		{
			GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
			database.TransactionalStorage.ClearCaches();
			GC.WaitForPendingFinalizers();
		}
	}
}
