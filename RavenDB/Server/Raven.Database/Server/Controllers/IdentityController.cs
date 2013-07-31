using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class IdentityController: RavenApiController
	{
		[HttpPost("identity")]
		public HttpResponseMessage IdentityNextPost()
		{
			var name = GetQueryStringValue("name");
			if (string.IsNullOrWhiteSpace(name))
			{
				return GetMessageWithObject(new
				{
					Error = "'name' query string parameter is mandatory and cannot be empty"
				}, HttpStatusCode.BadRequest);
			}

			long nextIdentityValue = -1;
			Database.TransactionalStorage.Batch(accessor =>
			{
				nextIdentityValue = accessor.General.GetNextIdentityValue(name);
			});

			return GetMessageWithObject(new {Value = nextIdentityValue});
		}
	}
}
