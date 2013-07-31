﻿using System.Net.Http;
using System.Web.Http;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("raven")]
	public class RavenUiController : RavenApiController
	{
		[HttpGet("")]
		public HttpResponseMessage RavenUiGet()
		{
			if (string.IsNullOrEmpty(Database.Configuration.RedirectStudioUrl) == false)
			{
				//TODO: redirect
				//context.Response.Redirect(Database.Configuration.RedirectStudioUrl);
				//return;
			}

			var docPath = GetRequestUrl().Replace("/raven/", "");
			return WriteEmbeddedFile(DatabasesLandlord.SystemConfiguration.WebDir, docPath);
		}
	}
}
