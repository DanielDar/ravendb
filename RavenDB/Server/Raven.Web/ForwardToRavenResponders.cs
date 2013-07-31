//-----------------------------------------------------------------------
// <copyright file="ForwardToRavenResponders.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Web;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.WebApi;

namespace Raven.Web
{
	public class ForwardToRavenResponders : IHttpHandler
	{
		private readonly WebApiServer server;

		public ForwardToRavenResponders(WebApiServer server)
		{
			this.server = server;
		}

		public void ProcessRequest(HttpContext context)
		{
			server.HandleActualRequest(new HttpContextAdapter(context, server.SystemConfiguration));
		}

		public bool IsReusable
		{
			get { return true; }
		}
	}
}