//-----------------------------------------------------------------------
// <copyright file="IStartupTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Raven.Database.Server;
using Raven.Database.Server.WebApi;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public interface IStartupTask
	{
		void Execute(DocumentDatabase database);
	}

	[InheritedExport]
	public interface IServerStartupTask
	{
		void Execute(WebApiServer server);
	}
}
