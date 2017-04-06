﻿using System;
using System.Threading.Tasks;
using Rebus.Logging;
using Rebus.SqlServerCe;
using Rebus.SqlServerCe.Subscriptions;
using Rebus.Subscriptions;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for subscriptions
    /// </summary>
    public static class SqlServerSubscriptionsConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use SQL Server to store subscriptions. Use <paramref name="isCentralized"/> = true to indicate whether it's OK to short-circuit
        /// subscribing and unsubscribing by manipulating the subscription directly from the subscriber or just let it default to false to preserve the
        /// default behavior.
        /// </summary>
        public static void StoreInSqlServer(this StandardConfigurer<ISubscriptionStorage> configurer,
            string connectionStringOrConnectionStringName, string tableName, bool isCentralized = false, bool automaticallyCreateTables = true)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionStringOrConnectionStringName == null) throw new ArgumentNullException(nameof(connectionStringOrConnectionStringName));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionProvider(connectionStringOrConnectionStringName, rebusLoggerFactory);
                var subscriptionStorage = new SqlServerSubscriptionStorage(connectionProvider, tableName, isCentralized, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    subscriptionStorage.EnsureTableIsCreated();
                }

                return subscriptionStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use SQL Server to store subscriptions. Use <paramref name="isCentralized"/> = true to indicate whether it's OK to short-circuit
        /// subscribing and unsubscribing by manipulating the subscription directly from the subscriber or just let it default to false to preserve the
        /// default behavior.
        /// </summary>
        public static void StoreInSqlServer(this StandardConfigurer<ISubscriptionStorage> configurer,
            Func<Task<IDbConnection>> connectionFactory, string tableName, bool isCentralized = false, bool automaticallyCreateTables = true)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionFactory == null) throw new ArgumentNullException(nameof(connectionFactory));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionFactoryProvider(connectionFactory, rebusLoggerFactory);
                var subscriptionStorage = new SqlServerSubscriptionStorage(connectionProvider, tableName, isCentralized, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    subscriptionStorage.EnsureTableIsCreated();
                }

                return subscriptionStorage;
            });
        }
    }
}