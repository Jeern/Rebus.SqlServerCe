using NUnit.Framework;
using Rebus.Logging;
using Rebus.SqlServerCe.Timeouts;
using Rebus.Tests.Contracts.Timeouts;
using Rebus.Timeouts;

namespace Rebus.SqlServerCe.Tests.Timeouts
{
    [TestFixture, Category(Categories.SqlServerCe)]
    public class BasicStoreAndRetrieveOperations : BasicStoreAndRetrieveOperations<SqlServerCeTimeoutManagerFactory>
    {
    }

    public class SqlServerCeTimeoutManagerFactory : ITimeoutManagerFactory
    {
        const string TableName = "RebusTimeouts";

        public ITimeoutManager Create()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(true);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, consoleLoggerFactory);
            var timeoutManager = new SqlServerCeTimeoutManager(connectionProvider, TableName, consoleLoggerFactory);

            timeoutManager.EnsureTableIsCreated();

            return timeoutManager;
        }

        public void Cleanup()
        {
            SqlTestHelper.DropTable(TableName);
        }

        public string GetDebugInfo()
        {
            return "could not provide debug info for this particular timeout manager.... implement if needed :)";
        }
    }
}