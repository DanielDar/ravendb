using System.Net.Http;
using System.Web.Http;
using Raven.Database.Commercial;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("license")]
	public class LicensingController : RavenApiController
	{
		[HttpGet("status")]
		public HttpResponseMessage LicenseStatusGet()
		{
			return GetMessageWithObject(ValidateLicense.CurrentLicense);				
		}
	}
}
