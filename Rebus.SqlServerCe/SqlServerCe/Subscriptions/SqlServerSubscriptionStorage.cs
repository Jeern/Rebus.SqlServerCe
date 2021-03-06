﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Logging;
using Rebus.SqlServerCe.Extensions;
using Rebus.Subscriptions;

namespace Rebus.SqlServerCe.Subscriptions
{
    /// <summary>
    /// Implementation of <see cref="ISubscriptionStorage"/> that persists subscriptions in a table in SQL Server Compact
    /// </summary>
    public class SqlServerCeSubscriptionStorage : ISubscriptionStorage, IInitializable
    {
        readonly IDbConnectionProvider _connectionProvider;
        readonly TableName _tableName;
        readonly ILog _log;

        int _topicLength = 200;
        int _addressLength = 200;

        /// <summary>
        /// Constructs the storage using the specified connection provider and table to store its subscriptions. If the subscription
        /// storage is shared by all subscribers and publishers, the <paramref name="isCentralized"/> parameter can be set to true
        /// in order to subscribe/unsubscribe directly instead of sending subscription/unsubscription requests
        /// </summary>
        public SqlServerCeSubscriptionStorage(IDbConnectionProvider connectionProvider, string tableName, bool isCentralized, IRebusLoggerFactory rebusLoggerFactory)
        {
            if (connectionProvider == null) throw new ArgumentNullException(nameof(connectionProvider));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));

            IsCentralized = isCentralized;

            _log = rebusLoggerFactory.GetLogger<SqlServerCeSubscriptionStorage>();
            _connectionProvider = connectionProvider;
            _tableName = new TableName(tableName);
        }

        /// <summary>
        /// Initializes the subscription storage by reading the lengths of the [topic] and [address] columns from SQL Server Compact
        /// </summary>
        public void Initialize()
        {
            try
            {
                using (var connection = _connectionProvider.GetConnection().Result)
                {
                    _topicLength = GetColumnWidth("topic", connection);
                    _addressLength = GetColumnWidth("address", connection);
                }
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, "Error during schema reflection");
            }
        }

        int GetColumnWidth(string columnName, IDbConnection connection)
        {
            var sql = $@"
SELECT 
    CHARACTER_MAXIMUM_LENGTH

FROM INFORMATION_SCHEMA.COLUMNS

WHERE 
    TABLE_NAME = '{_tableName.Name}'
    AND COLUMN_NAME = '{columnName}'
";

            try
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sql;
                    return (int) command.ExecuteScalar();
                }
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not get size of the [{columnName}] column from {_tableName} - executed SQL: '{sql}'");
            }
        }

        /// <summary>
        /// Creates the subscriptions table if necessary
        /// </summary>
        public void EnsureTableIsCreated()
        {
            using (var connection = _connectionProvider.GetConnection().Result)
            {
                var tableNames = connection.GetTableNames();

                if (tableNames.Contains(_tableName))
                {
                    return;
                }

                _log.Info("Table {tableName} does not exist - it will be created now", _tableName.Name);

                connection.TryExecuteCommands($@"
    CREATE TABLE {_tableName.Name} (
	    [topic] [nvarchar]({_topicLength}) NOT NULL,
	    [address] [nvarchar]({_addressLength}) NOT NULL
    )

----

CREATE UNIQUE INDEX [PK_{_tableName.Name}] ON {_tableName.Name} ([topic], [address])

");
  
                connection.Complete();
            }
        }

        /// <summary>
        /// Gets all destination addresses for the given topic
        /// </summary>
        public async Task<string[]> GetSubscriberAddresses(string topic)
        {
            using (var connection = await _connectionProvider.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"SELECT [address] FROM {_tableName.Name} WHERE [topic] = @topic";
                    command.Parameters.Add("topic", SqlDbType.NVarChar, _topicLength).Value = topic;

                    var subscriberAddresses = new List<string>();

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var address = (string)reader["address"];
                            subscriberAddresses.Add(address);
                        }
                    }

                    return subscriberAddresses.ToArray();
                }
            }
        }

        /// <summary>
        /// Registers the given <paramref name="subscriberAddress"/> as a subscriber of the given <paramref name="topic"/>
        /// </summary>
        public async Task RegisterSubscriber(string topic, string subscriberAddress)
        {
            CheckLengths(topic, subscriberAddress);

            using (var connection = await _connectionProvider.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
    INSERT INTO {_tableName.Name} ([topic], [address]) VALUES (@topic, @address)
";
                    command.Parameters.Add("topic", SqlDbType.NVarChar, _topicLength).Value = topic;
                    command.Parameters.Add("address", SqlDbType.NVarChar, _addressLength).Value = subscriberAddress;

                    await command.TryExecuteAsync();
                }

                await connection.Complete();
            }
        }

        void CheckLengths(string topic, string subscriberAddress)
        {
            if (topic.Length > _topicLength)
            {
                throw new ArgumentException(
                    $"Cannot register '{subscriberAddress}' as a subscriber of '{topic}' because the length of the topic is greater than {_topicLength} (which is the current MAX length allowed by the current {_tableName} schema)");
            }

            if (subscriberAddress.Length > _addressLength)
            {
                throw new ArgumentException(
                    $"Cannot register '{subscriberAddress}' as a subscriber of '{topic}' because the length of the subscriber address is greater than {_addressLength} (which is the current MAX length allowed by the current {_tableName} schema)");
            }
        }

        /// <summary>
        /// Unregisters the given <paramref name="subscriberAddress"/> as a subscriber of the given <paramref name="topic"/>
        /// </summary>
        public async Task UnregisterSubscriber(string topic, string subscriberAddress)
        {
            CheckLengths(topic, subscriberAddress);

            using (var connection = await _connectionProvider.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
DELETE FROM {_tableName.Name} WHERE [topic] = @topic AND [address] = @address
";
                    command.Parameters.Add("topic", SqlDbType.NVarChar, _topicLength).Value = topic;
                    command.Parameters.Add("address", SqlDbType.NVarChar, _addressLength).Value = subscriberAddress;

                    await command.ExecuteNonQueryAsync();
                }

                await connection.Complete();
            }
        }

        /// <summary>
        /// Gets whether this subscription storage is centralized
        /// </summary>
        public bool IsCentralized { get; }
    }
}