using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using System.Web.Http.Dispatcher;
using System.Web.Http.SelfHost;
using System.Web.Http.Services;
using Raven.Abstractions;
using Raven.Abstractions.Util;
using Raven.Database.Config;
using Raven.Database.Plugins.Builtins.Tenants;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Connections;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Responders;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi.Handlers;

namespace Raven.Database.Server.WebApi
{
	public class WebApiServer : IDisposable
	{
		private readonly InMemoryRavenConfiguration configuration;
		private readonly DocumentDatabase documentDatabase;
		private readonly HttpSelfHostConfiguration config;
		private readonly HttpSelfHostServer server;
		private DatabasesLandlord databasesLandlord;

		public WebApiServer(InMemoryRavenConfiguration configuration, DocumentDatabase documentDatabase)
		{
			this.configuration = configuration;
			this.documentDatabase = documentDatabase;

			databasesLandlord = new DatabasesLandlord(documentDatabase);	
			databasesLandlord.Initialize(this);
			config = new RavenSelfHostConfigurations(configuration.ServerUrl, databasesLandlord);
			config.Formatters.Remove(config.Formatters.XmlFormatter);

			config.Services.Replace(typeof(IAssembliesResolver), new MyAssemblyResolver());

			config.MapHttpAttributeRoutes();
			config.Routes.MapHttpRoute(
				"API Default", "{controller}/{action}",
				new { id = RouteParameter.Optional });

			config.Routes.MapHttpRoute(
				"Database Route", "databases/{databaseName}/{controller}/{action}",
				new { id = RouteParameter.Optional });
			config.MessageHandlers.Add(new GZipToJsonHandler());
			server = new HttpSelfHostServer(config);
		}

		public bool HasPendingRequests
		{
			get { return false; }//TODO: fix
		}

		public void Dispose()
		{
			if (server != null)
			{
				server.CloseAsync().Wait();
				server.Dispose();
			}
		}

		public Task StartListening()
		{
			return server.OpenAsync();
		}

		private int reqNum;
		public void ResetNumberOfRequests()
		{
			//TODO: implement method
			Interlocked.Exchange(ref reqNum, 0);
			Interlocked.Exchange(ref physicalRequestsCount, 0);
//#if DEBUG
//			while (recentRequests.Count > 0)
//			{
//				string _;
//				recentRequests.TryDequeue(out _);
//			}
//#endif
		}

		public static readonly Regex ChangesQuery = new Regex("^(/databases/([^/]+))?/changes/events", RegexOptions.IgnoreCase);


		private int physicalRequestsCount;
		public int NumberOfRequests
		{
			get { return Thread.VolatileRead(ref physicalRequestsCount); }
		}

		public Task<DocumentDatabase> GetDatabaseInternal(string name)
		{
			return databasesLandlord.GetDatabaseInternal(name);
		}

		internal class MyAssemblyResolver : IAssembliesResolver
		{
			public ICollection<Assembly> GetAssemblies()
			{
				return new[] { typeof(RavenApiController).Assembly };
			}
		}

		public DocumentDatabase SystemDatabase
		{
			get { return databasesLandlord.SystemDatabase; }
		}

		public InMemoryRavenConfiguration SystemConfiguration
		{
			get { return databasesLandlord.SystemConfiguration; }
		}

		private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim();
		private bool disposed;

		public void HandleActualRequest(IHttpContext context)
		{
			//TODO: implement
			var isReadLockHeld = disposerLock.IsReadLockHeld;
			if (isReadLockHeld == false)
				disposerLock.EnterReadLock();
			try
			{
				if (disposed)
					return;

				//if (IsWriteRequest(ctx))
				//{
				//	lastWriteRequest = SystemTime.UtcNow;
				//}
				var sw = Stopwatch.StartNew();
				bool ravenUiRequest = false;
				try
				{
					//ravenUiRequest = DispatchRequest(ctx);
				}
				catch (Exception e)
				{
					//ExceptionHandler.TryHandleException(ctx, e);
					//if (ShouldLogException(e))
					//	logger.WarnException("Error on request", e);
				}
				finally
				{
					try
					{
						//FinalizeRequestProcessing(ctx, sw, ravenUiRequest);
					}
					catch (Exception e)
					{
						//logger.ErrorException("Could not finalize request properly", e);
					}
				}
			}
			finally
			{
				if (isReadLockHeld == false)
					disposerLock.ExitReadLock();
			}
		}

		public Task HandleChangesRequest(IHttpContext httpContext, Func<bool> func)
		{
			//TODO: implement
			var sw = Stopwatch.StartNew();

			try
			{
				return new CompletedTask();
				//if (SetupRequestToProperDatabase() == false)
				//{
				//	FinalizeRequestSafe(context);
				//	onDisconnect();
				//	return new CompletedTask();
				//}

				//var eventsTransport = new EventsTransport(context);
				//eventsTransport.Disconnected += onDisconnect;
				//var handleChangesRequest = eventsTransport.ProcessAsync();
			//	return handleChangesRequest;
			}
			catch (Exception e)
			{
				//try
				//{
				//	ExceptionHandler.TryHandleException(context, e);
				//	LogException(e);
				//}
				//finally
				//{
				//	FinalizeRequestSafe(context);
				//}
				//onDisconnect();
				return new CompletedTask();
			}
			finally
			{
				//try
				//{
				//	LogHttpRequestStats(new LogHttpRequestStatsParams(
				//							sw,
				//							context.Request.Headers,
				//							context.Request.HttpMethod,
				//							context.Response.StatusCode,
				//							context.Request.Url.PathAndQuery));
				//}
				//catch (Exception e)
				//{
				//	logger.WarnException("Could not gather information to log request stats", e);
				//}
			}
		}

		private Timer serverTimer;
		private DateTime lastWriteRequest;
		private readonly TimeSpan frequencyToCheckForIdleDatabases = TimeSpan.FromMinutes(1);
		private readonly TimeSpan maxTimeDatabaseCanBeIdle;

		public void Init()
		{
			TenantDatabaseModified.Occured += databasesLandlord.TenantDatabaseRemoved;
			serverTimer = new Timer(IdleOperations, null, frequencyToCheckForIdleDatabases, frequencyToCheckForIdleDatabases);
		}

		private void IdleOperations(object state)
		{
			if ((SystemTime.UtcNow - lastWriteRequest).TotalMinutes < 1)
				return;// not idle, we just had a write request coming in

			try
			{
				SystemDatabase.RunIdleOperations();
			}
			catch (Exception e)
			{
				//TODO:Log
				//logger.ErrorException("Error during idle operation run for system database", e);
			}

			foreach (var documentDatabase in databasesLandlord.ResourcesStoresCache)
			{
				try
				{
					if (documentDatabase.Value.Status != TaskStatus.RanToCompletion)
						continue;
					documentDatabase.Value.Result.RunIdleOperations();
				}
				catch (Exception e)
				{
					//TODO: log
					//logger.WarnException("Error during idle operation run for " + documentDatabase.Key, e);
				}
			}

			var databasesToCleanup = databasesLandlord.DatabaseLastRecentlyUsed
				.Where(x => (SystemTime.UtcNow - x.Value) > maxTimeDatabaseCanBeIdle)
				.Select(x => x.Key)
				.ToArray();

			foreach (var db in databasesToCleanup)
			{
				// intentionally inside the loop, so we get better concurrency overall
				// since shutting down a database can take a while
				CleanupDatabase(db, skipIfActive: true);

			}
		}

		protected void CleanupDatabase(string db, bool skipIfActive)
		{
			databasesLandlord.CleanupDatabase(db, skipIfActive);
		}
	}
}
