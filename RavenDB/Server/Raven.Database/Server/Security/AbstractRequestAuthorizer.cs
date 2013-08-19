using System;
using Raven.Database.Server.WebApi;

namespace Raven.Database.Server.Security
{
	public abstract class AbstractRequestAuthorizer : IDisposable
	{
		[CLSCompliant(false)]
		protected DocumentDatabase database;
		[CLSCompliant(false)]
		protected WebApiServer server;


		public DocumentDatabase Database { get { return database; } }

		public void Initialize(DocumentDatabase database, WebApiServer theServer)
		{
			server = theServer;
			this.database = database;

			Initialize();
		}

		protected virtual void Initialize()
		{
		}

		public static bool IsGetRequest(string httpMethod, string requestPath)
		{
			return (httpMethod == "GET" || httpMethod == "HEAD") ||
				   httpMethod == "POST" && (requestPath == "/multi_get/" || requestPath == "/multi_get");
		}

		public abstract void Dispose();
	}
}