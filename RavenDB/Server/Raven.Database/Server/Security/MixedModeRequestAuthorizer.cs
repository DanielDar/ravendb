using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Security.Principal;
using System.Threading;
using System.Web;
using System.Web.Http;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Security.OAuth;
using Raven.Database.Server.Security.Windows;
using System.Linq;

namespace Raven.Database.Server.Security
{
	public class MixedModeRequestAuthorizer : AbstractRequestAuthorizer
	{
		private readonly WindowsRequestAuthorizer windowsRequestAuthorizer = new WindowsRequestAuthorizer();
		private readonly OAuthRequestAuthorizer oAuthRequestAuthorizer = new OAuthRequestAuthorizer();
		private readonly ConcurrentDictionary<string, OneTimeToken> singleUseAuthTokens = new ConcurrentDictionary<string, OneTimeToken>();

		private class OneTimeToken
		{
			private IPrincipal user;
			private IntPtr? windowsUserToken;
			public string DatabaseName { get; set; }
			public DateTime GeneratedAt { get; set; }
			public IPrincipal User
			{
				get
				{
					if (windowsUserToken != null)
					{
						return new WindowsPrincipal(new WindowsIdentity(windowsUserToken.Value));
					}
					return user;
				}
				set
				{
					var windowsPrincipal = value as WindowsPrincipal;
					if (windowsPrincipal != null)
					{
						user = null;
						windowsUserToken = ((WindowsIdentity)windowsPrincipal.Identity).Token;
						return;
					}
					windowsUserToken = null;
					user = value;
				}
			}
		}

		protected override void Initialize()
		{
			windowsRequestAuthorizer.Initialize(database, server);
			oAuthRequestAuthorizer.Initialize(database, server);
			base.Initialize();
		}

		public bool Authorize(RavenApiController controller)
		{
			var requestUrl = controller.GetRequestUrl();
			if ( NeverSecret.Urls.Contains(requestUrl))
				return true;

			var oneTimeToken = controller.GetHeader("Single-Use-Auth-Token");
			if (string.IsNullOrEmpty(oneTimeToken) == false)
			{
				return AuthorizeOSingleUseAuthToken(controller, oneTimeToken);
			}

			var authHeader = controller.GetHeader("Authorization");
			var hasApiKey = "True".Equals(controller.GetHeader("Has-Api-Key"), StringComparison.CurrentCultureIgnoreCase);
			var hasOAuthTokenInCookie = controller.HasCookie("OAuth-Token");
			if (hasApiKey || hasOAuthTokenInCookie || 
				string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
			{
				return oAuthRequestAuthorizer.Authorize(controller, hasApiKey, IgnoreDb.Urls.Contains(requestUrl));
			}
			return windowsRequestAuthorizer.Authorize(controller, IgnoreDb.Urls.Contains(requestUrl));
		}

		private bool AuthorizeOSingleUseAuthToken(RavenApiController controller, string token)
		{
			OneTimeToken value;
			if (singleUseAuthTokens.TryRemove(token, out value) == false)
			{
				var msg = controller.GetMessageWithObject(new
				{
					Error = "Unknown single use token, maybe it was already used?"
				}, HttpStatusCode.Forbidden);

				 throw new HttpResponseException(msg);
			}
			if (string.Equals(value.DatabaseName, controller.DatabaseName, StringComparison.InvariantCultureIgnoreCase) == false)
			{
				var msg = controller.GetMessageWithObject(new
				{
					Error = "This single use token cannot be used for this database"
				}, HttpStatusCode.Forbidden);

				throw new HttpResponseException(msg);
			}
			if ((SystemTime.UtcNow - value.GeneratedAt).TotalMinutes > 2.5)
			{
				var msg = controller.GetMessageWithObject(new
				{
					Error = "This single use token has expired"
				}, HttpStatusCode.Forbidden);

				throw new HttpResponseException(msg);
			}

			if (value.User != null)
			{
				CurrentOperationContext.Headers.Value[Constants.RavenAuthenticatedUser] = value.User.Identity.Name;
			}
			CurrentOperationContext.User.Value = value.User;
			HttpContext.Current.User = value.User;
			Thread.CurrentPrincipal = value.User;
			return true;
		}

		public IPrincipal GetUser(RavenApiController controller)
		{
			var hasApiKey = "True".Equals(controller.GetQueryStringValue("Has-Api-Key"), StringComparison.CurrentCultureIgnoreCase);
			var authHeader = controller.GetHeader("Authorization");
			var hasOAuthTokenInCookie = controller.HasCookie("OAuth-Token");
			if (hasApiKey || hasOAuthTokenInCookie ||
				string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
			{
				return oAuthRequestAuthorizer.GetUser(controller, hasApiKey);
			}
			return windowsRequestAuthorizer.GetUser(controller);
		}

		public List<string> GetApprovedDatabases(IPrincipal user, IHttpContext context)
		{
			var authHeader = context.Request.Headers["Authorization"];
			if (string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
			{
				return oAuthRequestAuthorizer.GetApprovedDatabases(user);
			}

			return windowsRequestAuthorizer.GetApprovedDatabases(user);
		}

		public override void Dispose()
		{
			windowsRequestAuthorizer.Dispose();
			oAuthRequestAuthorizer.Dispose();
		}

		public string GenerateSingleUseAuthToken(DocumentDatabase db, IPrincipal user, RavenApiController controller)
		{
			var token = new OneTimeToken
			{
				DatabaseName = controller.DatabaseName,
				GeneratedAt = SystemTime.UtcNow,
				User = user
			};
			var tokenString = Guid.NewGuid().ToString();

			singleUseAuthTokens.TryAdd(tokenString, token);

			if(singleUseAuthTokens.Count > 25)
			{
				foreach (var oneTimeToken in singleUseAuthTokens.Where(x => (x.Value.GeneratedAt - SystemTime.UtcNow).TotalMinutes > 5))
				{
					OneTimeToken value;
					singleUseAuthTokens.TryRemove(oneTimeToken.Key, out value);
				}
			}

			return tokenString;
		}
	}
}