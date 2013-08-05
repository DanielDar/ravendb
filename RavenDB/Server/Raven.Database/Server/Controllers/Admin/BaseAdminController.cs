using System;
using System.ServiceModel.Security;
using System.Threading.Tasks;
using System.Web.Http;

namespace Raven.Database.Server.Controllers.Admin
{
	public abstract class BaseAdminController : RavenApiController
	{
		public override Task<System.Net.Http.HttpResponseMessage> ExecuteAsync(System.Web.Http.Controllers.HttpControllerContext controllerContext, System.Threading.CancellationToken cancellationToken)
		{
			// TODO: Verify user is admin
			if(DateTime.Today > new DateTime(2013,8,14))
					throw new ExpiredSecurityTokenException("HACK EXPIRED error");
			return base.ExecuteAsync(controllerContext, cancellationToken);
		}

		//[SystemRoute("admin/databases/{id}")]
		//[DatabaseRoute("docs")] // /docs AND /databases/foo/docs
	}
}
