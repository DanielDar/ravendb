using System.Net.Http;
using System.Web.Http;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("transaction")]
	public class TransactionController : RavenApiController
	{
		[HttpPost("rollback")]
		public HttpResponseMessage Rollback()
		{
			var txId = GetQueryStringValue("tx");
			Database.Rollback(txId);
			return GetMessageWithObject(new { Rollbacked = txId });
		}

		[HttpGet("status")]
		public HttpResponseMessage Status()
		{
			var txId = GetQueryStringValue("tx");
			return GetMessageWithObject(new { Exists = Database.HasTransaction(txId) });
		}

		[HttpPost("prepare")]
		public HttpResponseMessage Prepare()
		{
			var txId = GetQueryStringValue("tx");

			Database.PrepareTransaction(txId);
			return GetMessageWithObject(new { Prepared = txId });
		}

		[HttpPost("commit")]
		public HttpResponseMessage Commit()
		{
			var txId = GetQueryStringValue("tx");
			Database.Commit(txId);
			return GetMessageWithObject(new { Committed = txId });
		}
	}
}
