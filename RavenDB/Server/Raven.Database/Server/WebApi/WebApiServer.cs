using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.Http.Dispatcher;
using System.Web.Http.SelfHost;
using System.Web.Http.Services;
using Raven.Database.Config;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Tenancy;

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

			config = new RavenSelfHostConfigurations("http://localhost:8080", databasesLandlord);
			config.Formatters.Remove(config.Formatters.XmlFormatter);

			config.Services.Replace(typeof(IAssembliesResolver), new MyAssemblyResolver());

			config.MapHttpAttributeRoutes();
			config.Routes.MapHttpRoute(
				"API Default", "{controller}/{action}",
				new { id = RouteParameter.Optional });

			//config.Routes.MapHttpRoute(
			//	"With Id", "{controller}/{action}/{id}",
			//	new { id = RouteParameter.Optional });

			config.Routes.MapHttpRoute(
				"Database Route", "databases/{databaseName}/{controller}/{action}",
				new { id = RouteParameter.Optional });

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

		internal class MyAssemblyResolver : IAssembliesResolver
		{
			public ICollection<Assembly> GetAssemblies()
			{
				return new[] { typeof(RavenApiController).Assembly };
			}
		}
	}
}
