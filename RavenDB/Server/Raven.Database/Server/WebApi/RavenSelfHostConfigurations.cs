using System;
using System.Web.Http.SelfHost;
using Raven.Database.Server.Tenancy;

namespace Raven.Database.Server.WebApi
{
	public class RavenSelfHostConfigurations : HttpSelfHostConfiguration
	{
		public readonly DatabasesLandlord Landlord;

		public RavenSelfHostConfigurations(string baseAddress, DatabasesLandlord landlord) : base(baseAddress)
		{
			Landlord = landlord;
		}

		public RavenSelfHostConfigurations(Uri baseAddress, DatabasesLandlord landlord)
			: base(baseAddress)
		{
			Landlord = landlord;
		}
	}
}
