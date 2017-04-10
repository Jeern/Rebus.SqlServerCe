using System.Data;
using System.Data.SqlClient;
using System.Data.SqlServerCe;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Logging;
using Rebus.SqlServerCe.Transport;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;
#pragma warning disable 1998

namespace Rebus.SqlServerCe.Tests.Integration
{
    [TestFixture]
    [Ignore("Can't run schema migration with snapshot isolation level")]
    public class SnapshotIsolationLevel : FixtureBase
    {
        const string TableName = "Messages";

        protected override void SetUp()
        {
            SqlTestHelper.DropTable(TableName);

            SqlTestHelper.Execute(@"
CREATE TABLE [Messages](
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[recipient] [nvarchar](200) NOT NULL,
	[priority] [int] NOT NULL,
	[expiration] [datetime](7) NOT NULL,
	[visible] [datetime](7) NOT NULL,
	[headers] [image] NOT NULL,
	[body] [image] NOT NULL
)");

            SqlTestHelper.Execute(@"
CREATE UNIQUE INDEX[PK_Messages] ON [Messages]([recipient], [priority], [id])
");

        }

        protected override void TearDown()
        {
        }

        [Test]
        public async Task ItWorks()
        {
            var activator = new BuiltinHandlerActivator();

            Using(activator);

            const int messageCount = 200;

            var counter = new SharedCounter(messageCount);

            activator.Handle<string>(async str =>
            {
                counter.Decrement();
            });

            Configure.With(activator)
                .Logging(l => l.Console(LogLevel.Info))
                .Transport(t =>
                {
                    var connectionString = SqlTestHelper.ConnectionString;

                    t.UseSqlServerCe(async () =>
                    {
                        var sqlConnection = new SqlCeConnection(connectionString);
                        await sqlConnection.OpenAsync();
                        var transaction = sqlConnection.BeginTransaction(IsolationLevel.Snapshot);
                        return new DbConnectionWrapper(sqlConnection, transaction, false);
                    }, TableName, "snapperino");
                })
                .Start();

            await Task.WhenAll(Enumerable.Range(0, messageCount)
                .Select(i => activator.Bus.SendLocal($"MAKE {i} GREAT AGAIN!!!!")));

            counter.WaitForResetEvent();
        }
    }
} 