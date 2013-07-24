using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	[RoutePrefix("databases/{databaseName}")]
	public class DatabasesController : RavenApiController
	{
		[HttpGet("databases")]
		public object Databases()
		{
			EnsureSystemDatabase();
				
			// This method is NOT secured, and anyone can access it.
			// Because of that, we need to provide explicit security here.

			// Anonymous Access - All / Get / Admin
			// Show all dbs

			// Anonymous Access - None
			// Show only the db that you have access to (read / read-write / admin)

			// If admin, show all dbs

			List<string> approvedDatabases = null;

			if (DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None)
			{
				var user = User;
				if (user == null)
				{
					return null;
				}

				if (user.IsAdministrator(DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode) == false)
				{
					//TODO: fix when request authorizer has been moved to the web api server
					//approvedDatabases = server.RequestAuthorizer.GetApprovedDatabases(user, context);
				}
			}

			var lastDocEtag = Etag.Empty;
			Database.TransactionalStorage.Batch(accessor =>
			{
				lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag();
			});

			if (MatchEtag(lastDocEtag))
			{
				//context.SetStatusToNotModified();
			}
			else
			{
				//context.WriteHeaders(new RavenJObject(), lastDocEtag);
				var databases = Database.GetDocumentsWithIdStartingWith("Raven/Databases/", null, GetStart(),
																		GetPageSize(Database.Configuration.MaxPageSize));
				var data = databases
					.Select(x => x.Value<RavenJObject>("@metadata").Value<string>("@id").Replace("Raven/Databases/", string.Empty))
					.ToArray();

				if (approvedDatabases != null)
				{
					data = data.Where(s => approvedDatabases.Contains(s)).ToArray();
				}

				return data;
			}

			return null;
		}

		[HttpGet("database/size")]
		public object DatabaseSize()
		{
			var totalSizeOnDisk = Database.GetTotalSizeOnDisk();
			return new
			{
				DatabaseSize = totalSizeOnDisk,
				DatabaseSizeHumane = Humane(totalSizeOnDisk)
			};
		}

		[HttpGet("database/storage/sizes")]
		public object DatabaseStorageSizes()
		{
			var indexStorageSize = Database.GetIndexStorageSizeOnDisk();
			var transactionalStorageSize = Database.GetTransactionalStorageSizeOnDisk();
			var totalDatabaseSize = indexStorageSize + transactionalStorageSize;
			return new
			{
				TransactionalStorageSize = transactionalStorageSize,
				TransactionalStorageSizeHumane = Humane(transactionalStorageSize),
				IndexStorageSize = indexStorageSize,
				IndexStorageSizeHumane = Humane(indexStorageSize),
				TotalDatabaseSize = totalDatabaseSize,
				TotalDatabaseSizeHumane = Humane(totalDatabaseSize),
			};
		}

		public static string Humane(long? size)
		{
			if (size == null)
				return null;

			var absSize = Math.Abs(size.Value);
			const double GB = 1024 * 1024 * 1024;
			const double MB = 1024 * 1024;
			const double KB = 1024;

			if (absSize > GB) // GB
				return string.Format("{0:#,#.##;;0} GBytes", size / GB);
			if (absSize > MB)
				return string.Format("{0:#,#.##;;0} MBytes", size / MB);
			if (absSize > KB)
				return string.Format("{0:#,#.##;;0} KBytes", size / KB);
			return string.Format("{0:#,#;;0} Bytes", size);

		}
	}
}
