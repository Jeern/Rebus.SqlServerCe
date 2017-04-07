using System;
using System.Threading.Tasks;
using Rebus.DataBus;
using Rebus.Logging;
using Rebus.SqlServerCe;
using Rebus.SqlServerCe.DataBus;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for SQL Server Compact data bus
    /// </summary>
    public static class SqlServerCeDataBusConfigurationExtensions
    {
        /// <summary>
        /// Configures the data bus to store data in SQL Server Compact
        /// </summary>
        public static void StoreInSqlServerCe(this StandardConfigurer<IDataBusStorage> configurer, string connectionStringOrConnectionStringName, string tableName, bool automaticallyCreateTables = true)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionStringOrConnectionStringName == null) throw new ArgumentNullException(nameof(connectionStringOrConnectionStringName));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            configurer.Register(c =>
            {
                var loggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionProvider(connectionStringOrConnectionStringName, loggerFactory);
                return new SqlServerCeDataBusStorage(connectionProvider, tableName, automaticallyCreateTables, loggerFactory);
            });
        }

        /// <summary>
        /// Configures the data bus to store data in a central SQL Server Compact
        /// </summary>
        public static void StoreInSqlServerCe(this StandardConfigurer<IDataBusStorage> configurer, Func<Task<IDbConnection>> connectionFactory, string tableName, bool automaticallyCreateTables = true)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionFactory == null) throw new ArgumentNullException(nameof(connectionFactory));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            configurer.Register(c =>
            {
                var loggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionFactoryProvider(connectionFactory, loggerFactory);
                return new SqlServerCeDataBusStorage(connectionProvider, tableName, automaticallyCreateTables, loggerFactory);
            });
        }
    }
}