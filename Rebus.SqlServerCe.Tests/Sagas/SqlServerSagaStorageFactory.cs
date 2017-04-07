using NUnit.Framework;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.SqlServerCe.Sagas;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.SqlServerCe.Tests.Sagas
{
    [TestFixture, Category(Categories.SqlServerCe)]
    public class SqlServerCeSagaStorageBasicLoadAndSaveAndFindOperations : BasicLoadAndSaveAndFindOperations<SqlServerCeSagaStorageFactory> { }

    [TestFixture, Category(Categories.SqlServerCe)]
    public class SqlServerCeSagaStorageConcurrencyHandling : ConcurrencyHandling<SqlServerCeSagaStorageFactory> { }

    [TestFixture, Category(Categories.SqlServerCe)]
    public class SqlServerCeSagaStorageSagaIntegrationTests : SagaIntegrationTests<SqlServerCeSagaStorageFactory> { }

    public class SqlServerCeSagaStorageFactory : ISagaStorageFactory
    {
        const string IndexTableName = "RebusSagaIndex";
        const string DataTableName = "RebusSagaData";

        public SqlServerCeSagaStorageFactory()
        {
            CleanUp();
        }

        public ISagaStorage GetSagaStorage()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(true);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, consoleLoggerFactory);
            var storage = new SqlServerCeSagaStorage(connectionProvider, DataTableName, IndexTableName, consoleLoggerFactory);

            storage.EnsureTablesAreCreated();

            return storage;
        }

        public void CleanUp()
        {
            SqlTestHelper.DropTable(IndexTableName);
            SqlTestHelper.DropTable(DataTableName);
        }
    }
}