using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rebus.Logging;
using Rebus.Serialization;
using Rebus.SqlServerCe.Extensions;
using Rebus.Time;
using Rebus.Timeouts;

namespace Rebus.SqlServerCe.Timeouts
{
    /// <summary>
    /// Implementation of <see cref="ITimeoutManager"/> that uses SQL Server Compact to store messages until it's time to deliver them.
    /// </summary>
    public class SqlServerCeTimeoutManager : ITimeoutManager
    {
        static readonly Encoding TextEncoding = Encoding.UTF8;
        static readonly HeaderSerializer HeaderSerializer = new HeaderSerializer();
        readonly IDbConnectionProvider _connectionProvider;
        readonly TableName _tableName;
        readonly ILog _log;

        /// <summary>
        /// Constructs the timeout manager, using the specified connection provider and table to store the messages until they're due.
        /// </summary>
        public SqlServerCeTimeoutManager(IDbConnectionProvider connectionProvider, string tableName, IRebusLoggerFactory rebusLoggerFactory)
        {
            if (connectionProvider == null) throw new ArgumentNullException(nameof(connectionProvider));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));

            _connectionProvider = connectionProvider;
            _tableName = new TableName(tableName);
            _log = rebusLoggerFactory.GetLogger<SqlServerCeTimeoutManager>();
        }

        /// <summary>
        /// Creates the due messages table if necessary
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

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
    CREATE TABLE {_tableName.Name} (
        [id] [int] IDENTITY(1,1) NOT NULL,
	    [due_time] [datetime](7) NOT NULL,
	    [headers] [image] NOT NULL,
	    [body] [image] NOT NULL,
        CONSTRAINT [PK_{_tableName.Name}] PRIMARY KEY NONCLUSTERED 
        (
	        [id] ASC
        )
    )
";
                    command.TryExecute();
                }

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
    CREATE CLUSTERED INDEX [IX_{_tableName.Name}_DueTime] ON {_tableName.Name}
    (
	    [due_time] ASC
    )";

                    command.TryExecute();
                }

                connection.Complete();
            }
        }

        /// <summary>
        /// Defers the message to the time specified by <paramref name="approximateDueTime"/> at which point in time the message will be
        /// returned to whoever calls <see cref="GetDueMessages"/>
        /// </summary>
        public async Task Defer(DateTimeOffset approximateDueTime, Dictionary<string, string> headers, byte[] body)
        {
            var headersString = HeaderSerializer.SerializeToString(headers);

            using (var connection = await _connectionProvider.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"INSERT INTO {_tableName.Name} ([due_time], [headers], [body]) VALUES (@due_time, @headers, @body)";

                    command.Parameters.Add("due_time", SqlDbType.DateTime).Value = approximateDueTime.UtcDateTime;
                    command.Parameters.Add("headers", SqlDbType.Image).Value = TextEncoding.GetBytes(headersString);
                    command.Parameters.Add("body", SqlDbType.Image).Value = body;

                    await command.ExecuteNonQueryAsync();
                }

                await connection.Complete();
            }
        }

        /// <summary>
        /// Gets messages due for delivery at the current time
        /// </summary>
        public async Task<DueMessagesResult> GetDueMessages()
        {
            var connection = await _connectionProvider.GetConnection();
            try
            {
                var dueMessages = new List<DueMessage>();

                const int maxDueTimeouts = 1000;

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
SELECT 
    [id],
    [headers],
    [body]
FROM {_tableName.Name} WITH (UPDLOCK, READPAST, ROWLOCK)
WHERE [due_time] <= @current_time 
ORDER BY [due_time] ASC
";

                    command.Parameters.Add("current_time", SqlDbType.DateTime).Value = RebusTime.Now.UtcDateTime;

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var id = (int)reader["id"];
                            var headersString = TextEncoding.GetString((byte[])reader["headers"]);
                            var headers = HeaderSerializer.DeserializeFromString(headersString);
                            var body = (byte[])reader["body"];

                            var sqlTimeout = new DueMessage(headers, body, async () =>
                            {
                                using (var deleteCommand = connection.CreateCommand())
                                {
                                    deleteCommand.CommandText = $"DELETE FROM {_tableName.Name} WHERE [id] = @id";
                                    deleteCommand.Parameters.Add("id", SqlDbType.Int).Value = id;
                                    await deleteCommand.ExecuteNonQueryAsync();
                                }
                            });

                            dueMessages.Add(sqlTimeout);

                            if (dueMessages.Count >= maxDueTimeouts) break;
                        }
                    }

                    return new DueMessagesResult(dueMessages, async () =>
                    {
                        using (connection)
                        {
                            await connection.Complete();
                        }
                    });
                }
            }
            catch (Exception)
            {
                connection.Dispose();
                throw;
            }
        }
    }
}