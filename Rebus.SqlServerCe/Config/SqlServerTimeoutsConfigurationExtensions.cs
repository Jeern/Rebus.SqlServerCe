﻿using System;
using System.Threading.Tasks;
using Rebus.Logging;
using Rebus.SqlServerCe;
using Rebus.SqlServerCe.Timeouts;
using Rebus.Timeouts;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for configuring SQL persistence for sagas, subscriptions, and timeouts.
    /// </summary>
    public static class SqlServerCeTimeoutsConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use SQL Server Compact to store timeouts.
        /// </summary>
        public static void StoreInSqlServerCe(this StandardConfigurer<ITimeoutManager> configurer, 
            string connectionStringOrConnectionStringName, string tableName, bool automaticallyCreateTables = true)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionStringOrConnectionStringName == null) throw new ArgumentNullException(nameof(connectionStringOrConnectionStringName));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionProvider(connectionStringOrConnectionStringName, rebusLoggerFactory);
                var subscriptionStorage = new SqlServerCeTimeoutManager(connectionProvider, tableName, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    subscriptionStorage.EnsureTableIsCreated();
                }

                return subscriptionStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use SQL Server Compact to store timeouts.
        /// </summary>
        public static void StoreInSqlServerCe(this StandardConfigurer<ITimeoutManager> configurer,
            Func<Task<IDbConnection>> connectionFactory, string tableName, bool automaticallyCreateTables = true)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionFactory == null) throw new ArgumentNullException(nameof(connectionFactory));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionFactoryProvider(connectionFactory, rebusLoggerFactory);
                var subscriptionStorage = new SqlServerCeTimeoutManager(connectionProvider, tableName, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    subscriptionStorage.EnsureTableIsCreated();
                }

                return subscriptionStorage;
            });
        }
    }
}