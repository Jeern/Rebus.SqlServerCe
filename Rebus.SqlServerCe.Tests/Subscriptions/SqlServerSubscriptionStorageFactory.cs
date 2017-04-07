using Rebus.Logging;
using Rebus.SqlServerCe.Subscriptions;
using Rebus.Subscriptions;
using Rebus.Tests.Contracts.Subscriptions;

namespace Rebus.SqlServerCe.Tests.Subscriptions
{
    public class SqlServerCeSubscriptionStorageFactory : ISubscriptionStorageFactory
    {
        const string TableName = "RebusSubscriptions";
        
        public ISubscriptionStorage Create()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(true);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, consoleLoggerFactory);
            var storage = new SqlServerCeSubscriptionStorage(connectionProvider, TableName, true, consoleLoggerFactory);

            storage.EnsureTableIsCreated();
            
            return storage;
        }

        public void Cleanup()
        {
            SqlTestHelper.DropTable(TableName);
        }
    }
}