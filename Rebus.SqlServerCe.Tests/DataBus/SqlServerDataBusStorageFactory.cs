using Rebus.DataBus;
using Rebus.Logging;
using Rebus.SqlServerCe.DataBus;
using Rebus.Tests.Contracts.DataBus;

namespace Rebus.SqlServerCe.Tests.DataBus
{
    public class SqlServerCeDataBusStorageFactory : IDataBusStorageFactory
    {
        public SqlServerCeDataBusStorageFactory()
        {
            SqlTestHelper.DropTable("databus");
        }

        public IDataBusStorage Create()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, consoleLoggerFactory);
            var SqlServerCeDataBusStorage = new SqlServerCeDataBusStorage(connectionProvider, "databus", true, consoleLoggerFactory);
            SqlServerCeDataBusStorage.Initialize();
            return SqlServerCeDataBusStorage;
        }

        public void CleanUp()
        {
            SqlTestHelper.DropTable("databus");
        }
    }
}