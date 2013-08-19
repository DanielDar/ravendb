using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Security.Principal;
using System.Linq;
using System.Threading;
using System.Web;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Server.Controllers;

namespace Raven.Database.Server.Security.OAuth
{
	public class OAuthRequestAuthorizer : AbstractRequestAuthorizer
	{
		public bool Authorize(RavenApiController controller, bool hasApiKey, bool ignoreDbAccess)
		{
		//	var httpRequest = ctx.Request;

			var isGetRequest = IsGetRequest(controller.Request.Method.Method, controller.Request.RequestUri.AbsolutePath);
			var allowUnauthenticatedUsers = // we need to auth even if we don't have to, for bundles that want the user 
				controller.DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.All ||
				controller.DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.Admin ||
					controller.DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.Get &&
			        isGetRequest;

			var token = GetToken(controller);
			
			if (token == null)
			{
				if (allowUnauthenticatedUsers)
					return true;

				WriteAuthorizationChallenge(controller, hasApiKey ? 412 : 401, "invalid_request", "The access token is required");
				
				return false;
			}

			AccessTokenBody tokenBody;
			if (!AccessToken.TryParseBody(controller.DatabasesLandlord.SystemConfiguration.OAuthTokenKey, token, out tokenBody))
			{
				if (allowUnauthenticatedUsers)
					return true;
				WriteAuthorizationChallenge(controller, 401, "invalid_token", "The access token is invalid");

				return false;
			}

			if (tokenBody.IsExpired())
			{
				if (allowUnauthenticatedUsers)
					return true;
				WriteAuthorizationChallenge(controller, 401, "invalid_token", "The access token is expired");

				return false;
			}

			var writeAccess = isGetRequest == false;
			if(!tokenBody.IsAuthorized(controller.DatabaseName, writeAccess))
			{
				if (allowUnauthenticatedUsers || ignoreDbAccess)
					return true;

				WriteAuthorizationChallenge(controller, 403, "insufficient_scope", 
					writeAccess ?
					"Not authorized for read/write access for tenant " + controller.DatabaseName :
					"Not authorized for tenant " + controller.DatabaseName);
	   
				return false;
			}

			HttpContext.Current.User = new OAuthPrincipal(tokenBody, controller.DatabaseName);
			Thread.CurrentPrincipal = new OAuthPrincipal(tokenBody, controller.DatabaseName);

			CurrentOperationContext.Headers.Value[Constants.RavenAuthenticatedUser] = tokenBody.UserId;
			CurrentOperationContext.User.Value = controller.User;
			return true;
		}

		public List<string> GetApprovedDatabases(IPrincipal user)
		{
			var oAuthUser = user as OAuthPrincipal;
			if (oAuthUser == null)
				return new List<string>();
			return oAuthUser.GetApprovedDatabases();
		}

		public override void Dispose()
		{
			
		}

		static string GetToken(RavenApiController controller)
		{
			const string bearerPrefix = "Bearer ";

			var auth = controller.GetHeader("Authorization");
			if(auth == null)
			{
				auth = controller.GetCookie("OAuth-Token");
				if (auth != null)
					auth = Uri.UnescapeDataString(auth);
			}
			if (auth == null || auth.Length <= bearerPrefix.Length ||
				!auth.StartsWith(bearerPrefix, StringComparison.OrdinalIgnoreCase))
				return null;

			var token = auth.Substring(bearerPrefix.Length, auth.Length - bearerPrefix.Length);
			
			return token;
		}

		void WriteAuthorizationChallenge(RavenApiController controller, int statusCode, string error, string errorDescription)
		{
			var msg = new HttpResponseMessage();

			if (string.IsNullOrEmpty(controller.DatabasesLandlord.SystemConfiguration.OAuthTokenServer) == false)
			{
				if (controller.DatabasesLandlord.SystemConfiguration.UseDefaultOAuthTokenServer == false)
				{
					controller.AddHeader("OAuth-Source", controller.DatabasesLandlord.SystemConfiguration.OAuthTokenServer, msg);
				}
				else
				{
					controller.AddHeader("OAuth-Source", new UriBuilder(controller.DatabasesLandlord.SystemConfiguration.OAuthTokenServer)
					{
						Host = controller.Request.RequestUri.Host,
						Port = controller.Request.RequestUri.Port
					}.Uri.ToString(), msg);
			
				}
			}
			msg.StatusCode = (HttpStatusCode) statusCode;
			controller.AddHeader("WWW-Authenticate", string.Format("Bearer realm=\"Raven\", error=\"{0}\",error_description=\"{1}\"", error, errorDescription), msg);

			throw new HttpResponseException(msg);
		}

		public IPrincipal GetUser(RavenApiController controller, bool hasApiKey)
		{
			var token = GetToken(controller);

			if (token == null)
			{
				WriteAuthorizationChallenge(controller, hasApiKey ? 412 : 401, "invalid_request", "The access token is required");

				return null;
			}

			AccessTokenBody tokenBody;
			if (!AccessToken.TryParseBody(controller.DatabasesLandlord.SystemConfiguration.OAuthTokenKey, token, out tokenBody))
			{
				WriteAuthorizationChallenge(controller, 401, "invalid_token", "The access token is invalid");

				return null;
			}

			return new OAuthPrincipal(tokenBody, null);
		}
	}

	public class OAuthPrincipal : IPrincipal, IIdentity
	{
		private readonly AccessTokenBody tokenBody;
		private readonly string tenantId;

		public OAuthPrincipal(AccessTokenBody tokenBody, string tenantId)
		{
			this.tokenBody = tokenBody;
			this.tenantId = tenantId;
		}

		public bool IsInRole(string role)
		{
			if ("Administrators".Equals(role, StringComparison.OrdinalIgnoreCase) == false)
				return false;

			var databaseAccess = tokenBody.AuthorizedDatabases
				.Where(x=>
					string.Equals(x.TenantId, tenantId, StringComparison.OrdinalIgnoreCase) ||
					x.TenantId == "*");

			return databaseAccess.Any(access => access.Admin);
		}

		public IIdentity Identity
		{
			get { return this; }
		}

		public string Name
		{
			get { return tokenBody.UserId; }
		}

		public string AuthenticationType
		{
			get { return "OAuth"; }
		}

		public bool IsAuthenticated
		{
			get { return true; }
		}

		public List<string> GetApprovedDatabases()
		{
			return tokenBody.AuthorizedDatabases.Select(access => access.TenantId).ToList();
		}

		public AccessTokenBody TokenBody
		{
			get { return tokenBody; }
		}
	}
}
