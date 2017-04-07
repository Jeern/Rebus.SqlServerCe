﻿using System;
using System.Threading.Tasks;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.SqlServerCe;
using Rebus.SqlServerCe.Sagas;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for sagas
    /// </summary>
    public static class SqlServerCeSagaConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use SQL Server Compact to store sagas, using the tables specified to store data and indexed properties respectively.
        /// </summary>
        public static void StoreInSqlServerCe(this StandardConfigurer<ISagaStorage> configurer,
            string connectionStringOrConnectionStringName, string dataTableName, string indexTableName,
            bool automaticallyCreateTables = true)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionStringOrConnectionStringName == null) throw new ArgumentNullException(nameof(connectionStringOrConnectionStringName));
            if (dataTableName == null) throw new ArgumentNullException(nameof(dataTableName));
            if (indexTableName == null) throw new ArgumentNullException(nameof(indexTableName));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionProvider(connectionStringOrConnectionStringName, rebusLoggerFactory);
                var sagaStorage = new SqlServerCeSagaStorage(connectionProvider, dataTableName, indexTableName, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    sagaStorage.EnsureTablesAreCreated();
                }

                return sagaStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use SQL Server Compact to store sagas, using the tables specified to store data and indexed properties respectively.
        /// </summary>
        public static void StoreInSqlServerCe(this StandardConfigurer<ISagaStorage> configurer,
            Func<Task<IDbConnection>> connectionFactory, string dataTableName, string indexTableName,
            bool automaticallyCreateTables = true)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionFactory == null) throw new ArgumentNullException(nameof(connectionFactory));
            if (dataTableName == null) throw new ArgumentNullException(nameof(dataTableName));
            if (indexTableName == null) throw new ArgumentNullException(nameof(indexTableName));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionFactoryProvider(connectionFactory, rebusLoggerFactory);
                var sagaStorage = new SqlServerCeSagaStorage(connectionProvider, dataTableName, indexTableName, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    sagaStorage.EnsureTablesAreCreated();
                }

                return sagaStorage;
            });
        }
    }
}