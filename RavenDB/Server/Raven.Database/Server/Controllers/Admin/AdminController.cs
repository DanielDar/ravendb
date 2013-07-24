using System;
using System.Net;
using System.Net.Http.Formatting;
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
		public async Task<HttpResponseMessage> Backup()
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
		public HttpResponseMessage ChangeDbId()
		{
			Guid old = Database.TransactionalStorage.Id;
			var newId = Database.TransactionalStorage.ChangeId();

			return GetMessageWithObject(new
			{
				OldId = old,
				NewId = newId
			});
		}

		[HttpGet("compact")]
		public HttpResponseMessage Compact()
		{
			EnsureSystemDatabase();
				
			var db = Request.RequestUri.ParseQueryString()["database"];
			if (string.IsNullOrWhiteSpace(db))
				return GetMessageWithString("Compact request requires a valid database parameter", HttpStatusCode.BadRequest);

			var configuration = DatabasesLandlord.CreateTenantConfiguration(db);
			if (configuration == null)
				return GetMessageWithString("No database named: " + db, HttpStatusCode.NotFound);

			DatabasesLandlord.LockDatabase(db, () => DatabasesLandlord.SystemDatabase.TransactionalStorage.Compact(configuration));

			return new HttpResponseMessage(HttpStatusCode.OK);
		}

		[HttpGet("indexingStatus")]
		public HttpResponseMessage IndexingStatus()
		{
			return GetMessageWithObject(new {IndexingStatus = Database.WorkContext.RunIndexing ? "Indexing" : "Paused"});		
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
		public HttpResponseMessage Stats()
		{
			if (Database != DatabasesLandlord.SystemDatabase)
				return GetMessageWithString("Admin stats can only be had from the root database", HttpStatusCode.NotFound);

			return GetMessageWithObject(DatabasesLandlord.SystemDatabase.Statistics);
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