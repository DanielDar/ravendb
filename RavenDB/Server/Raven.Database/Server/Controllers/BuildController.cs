using System.Web.Http;

namespace Raven.Database.Server.Controllers
{
	public class BuildController : RavenApiController
	{
		[HttpGet]
		public object Version()
		{
			return new
			{
				ProductVersion = "abc",
				BuildVersion = "1234",
				DatabaseName = DatabaseName
			};
		}
	}
}
