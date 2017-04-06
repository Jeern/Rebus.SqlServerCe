﻿using Rebus.DataBus;
using Rebus.Logging;
using Rebus.SqlServerCe.DataBus;
using Rebus.Tests.Contracts.DataBus;

namespace Rebus.SqlServerCe.Tests.DataBus
{
    public class SqlServerDataBusStorageFactory : IDataBusStorageFactory
    {
        public SqlServerDataBusStorageFactory()
        {
            SqlTestHelper.DropTable("databus");
        }

        public IDataBusStorage Create()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, consoleLoggerFactory);
            var sqlServerDataBusStorage = new SqlServerDataBusStorage(connectionProvider, "databus", true, consoleLoggerFactory);
            sqlServerDataBusStorage.Initialize();
            return sqlServerDataBusStorage;
        }

        public void CleanUp()
        {
            SqlTestHelper.DropTable("databus");
        }
    }
}