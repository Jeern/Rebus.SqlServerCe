﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlServerCe;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Serialization;
using Rebus.SqlServerCe.Extensions;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.SqlServerCe.Transport
{
    /// <summary>
    /// Implementation of <see cref="ITransport"/> that uses SQL Server Compact to do its thing
    /// </summary>
    public class SqlServerCeTransport : ITransport, IInitializable, IDisposable
    {
        static readonly HeaderSerializer HeaderSerializer = new HeaderSerializer();

        /// <summary>
        /// When a message is sent to this address, it will be deferred into the future!
        /// </summary>
        public const string MagicExternalTimeoutManagerAddress = "##### MagicExternalTimeoutManagerAddress #####";

        /// <summary>
        /// Special message priority header that can be used with the <see cref="SqlServerCeTransport"/>. The value must be an <see cref="Int32"/>
        /// </summary>
        public const string MessagePriorityHeaderKey = "rbs2-msg-priority";

        /// <summary>
        /// Default interval that will be used for <see cref="ExpiredMessagesCleanupInterval"/> unless it is explicitly set to something else
        /// </summary>
        public static readonly TimeSpan DefaultExpiredMessagesCleanupInterval = TimeSpan.FromSeconds(20);

        const string CurrentConnectionKey = "sql-server-transport-current-connection";
        const int RecipientColumnSize = 200;

        readonly AsyncBottleneck _bottleneck = new AsyncBottleneck(20);
        readonly IDbConnectionProvider _connectionProvider;
        readonly TableName _tableName;
        readonly string _inputQueueName;
        readonly ILog _log;

        readonly IAsyncTask _expiredMessagesCleanupTask;
        bool _disposed;

        /// <summary>
        /// Constructs the transport with the given <see cref="IDbConnectionProvider"/>, using the specified <paramref name="tableName"/> to send/receive messages,
        /// querying for messages with recipient = <paramref name="inputQueueName"/>
        /// </summary>
        public SqlServerCeTransport(IDbConnectionProvider connectionProvider, string tableName, string inputQueueName, IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory)
        {
            if (connectionProvider == null) throw new ArgumentNullException(nameof(connectionProvider));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            if (asyncTaskFactory == null) throw new ArgumentNullException(nameof(asyncTaskFactory));

            _connectionProvider = connectionProvider;
            _tableName = new TableName(tableName);
            _inputQueueName = inputQueueName;
            _log = rebusLoggerFactory.GetLogger<SqlServerCeTransport>();

            ExpiredMessagesCleanupInterval = DefaultExpiredMessagesCleanupInterval;

            _expiredMessagesCleanupTask = asyncTaskFactory.Create("ExpiredMessagesCleanup", PerformExpiredMessagesCleanupCycle, intervalSeconds: 60);
        }

        /// <summary>
        /// Initializes the transport by starting a task that deletes expired messages from the SQL table
        /// </summary>
        public void Initialize()
        {
            if (_inputQueueName == null) return;

            _expiredMessagesCleanupTask.Start();
        }

        /// <summary>
        /// Configures the interval between periodic deletion of expired messages. Defaults to <see cref="DefaultExpiredMessagesCleanupInterval"/>
        /// </summary>
        public TimeSpan ExpiredMessagesCleanupInterval { get; set; }

        /// <summary>
        /// Gets the name that this SQL transport will use to query by when checking the messages table
        /// </summary>
        public string Address => _inputQueueName;

        /// <summary>
        /// The SQL transport doesn't really have queues, so this function does nothing
        /// </summary>
        public void CreateQueue(string address)
        {
        }

        /// <summary>
        /// Checks if the table with the configured name exists - if not, it will be created
        /// </summary>
        public void EnsureTableIsCreated()
        {
            try
            {
                CreateSchema();
            }
            catch (SqlCeException exception)
            {
                throw new RebusApplicationException(exception, $"Error attempting to initialize SQL transport schema with mesages table {_tableName.Name}");
            }
        }

        void CreateSchema()
        {
            using (var connection = _connectionProvider.GetConnection().Result)
            {
                var tableNames = connection.GetTableNames();
                
                if (tableNames.Contains(_tableName))
                {
                    _log.Info("Database already contains a table named {tableName} - will not create anything", _tableName.Name);
                    return;
                }

                _log.Info("Table {tableName} does not exist - it will be created now", _tableName.Name);

                var receiveIndexName = $"IDX_RECEIVE_{_tableName.Name}";
                var expirationIndexName = $"IDX_EXPIRATION_{_tableName.Name}";

                connection.TryExecuteCommands($@"
    CREATE TABLE {_tableName.Name}
    (
	    [id] [bigint] IDENTITY(1,1) NOT NULL,
	    [recipient] [nvarchar](200) NOT NULL,
	    [priority] [int] NOT NULL,
        [expiration] [datetime] NOT NULL,
        [visible] [datetime] NOT NULL,
	    [headers] [image] NOT NULL,
	    [body] [image] NOT NULL
    )

----

CREATE UNIQUE INDEX [PK_{_tableName.Name}] ON {_tableName.Name} ([recipient], [priority], [id])

----

    CREATE NONCLUSTERED INDEX [{receiveIndexName}] ON {_tableName.Name}
    (
	    [recipient] ASC,
	    [priority] ASC,
        [visible] ASC,
        [expiration] ASC,
	    [id] ASC
    )

----

    CREATE NONCLUSTERED INDEX [{expirationIndexName}] ON {_tableName.Name}
    (
        [expiration] ASC
    )

");

                connection.Complete().Wait();
            }
        }


        /// <summary>
        /// Sends the given transport message to the specified logical destination address by adding it to the messages table.
        /// </summary>
        public async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            var connection = await GetConnection(context);

            var destinationAddressToUse = string.Equals(destinationAddress, MagicExternalTimeoutManagerAddress, StringComparison.CurrentCultureIgnoreCase)
                ? GetDeferredRecipient(message)
                : destinationAddress;

            await InnerSend(destinationAddressToUse, message, connection);
        }

        /// <summary>
        /// Receives the next message by querying the messages table for a message with a recipient matching this transport's <see cref="Address"/>
        /// </summary>
        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            using (await _bottleneck.Enter(cancellationToken))
            {
                var connection = await GetConnection(context);

                TransportMessage receivedTransportMessage;


                //VI ER HER - DENNE COMMAND SKAL OMSKRIVES TIL MULTIPLE

                using (var selectCommand = connection.CreateCommand())
                using (var deleteCommand = connection.CreateCommand())
                {
                    selectCommand.CommandText = $@"
		SELECT	TOP 1
				[id],
				[headers],
				[body]
		FROM	{_tableName.Name} M 
		WHERE	M.[recipient] = @recipient
		AND		M.[visible] < getdate()
		AND		M.[expiration] > getdate()
		ORDER
		BY		[priority] ASC,
				[id] ASC";

                    deleteCommand.CommandText = $@"DELETE FROM {_tableName.Name} WHERE [id] = @id";
                    selectCommand.Parameters.Add("recipient", SqlDbType.NVarChar, RecipientColumnSize).Value = _inputQueueName;

                    try
                    {
                        long id;
                        using (var reader = await selectCommand.ExecuteReaderAsync(cancellationToken))
                        {
                            if (!await reader.ReadAsync(cancellationToken)) return null;

                            var headers = reader["headers"];
                            var headersDictionary = HeaderSerializer.Deserialize((byte[]) headers);
                            var body = (byte[]) reader["body"];
                            id = (long) reader["id"];


                            receivedTransportMessage = new TransportMessage(headersDictionary, body);
                        }
                        deleteCommand.Parameters.Add("id", SqlDbType.BigInt).Value = id;
                        await deleteCommand.ExecuteNonQueryAsync(cancellationToken);
                    }
                    catch (Exception exception) when (cancellationToken.IsCancellationRequested)
                    {
                        // ADO.NET does not throw the right exception when the task gets cancelled - therefore we need to do this:
                        throw new TaskCanceledException("Receive operation was cancelled", exception);
                    }
                }

                return receivedTransportMessage;
            }
        }

        static string GetDeferredRecipient(TransportMessage message)
        {
            string destination;

            if (message.Headers.TryGetValue(Headers.DeferredRecipient, out destination))
            {
                return destination;
            }

            throw new InvalidOperationException($"Attempted to defer message, but no '{Headers.DeferredRecipient}' header was on the message");
        }

        async Task InnerSend(string destinationAddress, TransportMessage message, IDbConnection connection)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"
INSERT INTO {_tableName.Name}
(
    [recipient],
    [headers],
    [body],
    [priority],
    [visible],
    [expiration]
)
VALUES
(
    @recipient,
    @headers,
    @body,
    @priority,
    dateadd(ss, @visible, getdate()),
    dateadd(ss, @ttlseconds, getdate())
)";

                var headers = message.Headers.Clone();

                var priority = GetMessagePriority(headers);
                var initialVisibilityDelay = GetInitialVisibilityDelay(headers);
                var ttlSeconds = GetTtlSeconds(headers);

                // must be last because the other functions on the headers might change them
                var serializedHeaders = HeaderSerializer.Serialize(headers);

                command.Parameters.Add("recipient", SqlDbType.NVarChar, RecipientColumnSize).Value = destinationAddress;
                command.Parameters.Add("headers", SqlDbType.Image).Value = serializedHeaders;
                command.Parameters.Add("body", SqlDbType.Image).Value = message.Body;
                command.Parameters.Add("priority", SqlDbType.Int).Value = priority;
                command.Parameters.Add("ttlseconds", SqlDbType.Int).Value = ttlSeconds;
                command.Parameters.Add("visible", SqlDbType.Int).Value = initialVisibilityDelay;

                await command.ExecuteNonQueryAsync();
            }
        }

        static int GetInitialVisibilityDelay(IDictionary<string, string> headers)
        {
            string deferredUntilDateTimeOffsetString;

            if (!headers.TryGetValue(Headers.DeferredUntil, out deferredUntilDateTimeOffsetString))
            {
                return 0;
            }

            var deferredUntilTime = deferredUntilDateTimeOffsetString.ToDateTimeOffset();

            headers.Remove(Headers.DeferredUntil);

            return (int)(deferredUntilTime - RebusTime.Now).TotalSeconds;
        }

        static int GetTtlSeconds(IReadOnlyDictionary<string, string> headers)
        {
            const int defaultTtlSecondsAbout60Years = int.MaxValue;

            if (!headers.ContainsKey(Headers.TimeToBeReceived))
                return defaultTtlSecondsAbout60Years;

            var timeToBeReceivedStr = headers[Headers.TimeToBeReceived];
            var timeToBeReceived = TimeSpan.Parse(timeToBeReceivedStr);

            return (int)timeToBeReceived.TotalSeconds;
        }

        async Task PerformExpiredMessagesCleanupCycle()
        {
            var results = 0;
            var stopwatch = Stopwatch.StartNew();

            while (true)
            {
                using (var connection = await _connectionProvider.GetConnection())
                {
                    int affectedRows;

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText =
                            $@"
;with TopCTE as (
	SELECT TOP 1 [id] FROM {_tableName.Name} WITH (ROWLOCK, READPAST)
				WHERE [recipient] = @recipient 
					AND [expiration] < getdate()
)
DELETE FROM TopCTE
";
                        command.Parameters.Add("recipient", SqlDbType.NVarChar, RecipientColumnSize).Value = _inputQueueName;

                        affectedRows = await command.ExecuteNonQueryAsync();
                    }

                    results += affectedRows;

                    await connection.Complete();

                    if (affectedRows == 0) break;
                }
            }

            if (results > 0)
            {
                _log.Info("Performed expired messages cleanup in {cleanupTimeSeconds} - {expiredMessageCount} expired messages with recipient {queueName} were deleted",
                    stopwatch.Elapsed.TotalSeconds, results, _inputQueueName);
            }
        }

        static int GetMessagePriority(Dictionary<string, string> headers)
        {
            var valueOrNull = headers.GetValueOrNull(MessagePriorityHeaderKey);
            if (valueOrNull == null) return 0;

            try
            {
                return int.Parse(valueOrNull);
            }
            catch (Exception exception)
            {
                throw new FormatException($"Could not parse '{valueOrNull}' into an Int32!", exception);
            }
        }

        Task<IDbConnection> GetConnection(ITransactionContext context)
        {
            return context
                .GetOrAdd(CurrentConnectionKey,
                    async () =>
                    {
                        var dbConnection = await _connectionProvider.GetConnection();
                        context.OnCommitted(async () => await dbConnection.Complete());
                        context.OnDisposed(() =>
                        {
                            dbConnection.Dispose();
                        });
                        return dbConnection;
                    });
        }

        /// <summary>
        /// Shuts down the background timer
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;

            try
            {
                _expiredMessagesCleanupTask.Dispose();
            }
            finally
            {
                _disposed = true;
            }
        }
    }
}