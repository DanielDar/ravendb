using System.Threading.Tasks;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs.Async
{
	public class Querying : RemoteClientTest
	{
		[Fact]
		public async Task Can_query_using_async_session()
		{
			using (GetNewServer())
			using (var store = new DocumentStore {Url = "http://localhost:8079"}.Initialize())
			{
				using (var s = store.OpenAsyncSession())
				{
					await s.StoreAsync(new {Name = "Ayende"});
					await s.SaveChangesAsync();
				}

				using (var s = store.OpenAsyncSession())
				{
					var queryResultAsync = await s.Advanced.AsyncLuceneQuery<dynamic>()
					                              .WhereEquals("Name", "Ayende")
					                              .ToListAsync();

					var result = queryResultAsync.Item2;
					Assert.Equal("Ayende", result[0].Name);
				}
			}
		}
	}
}