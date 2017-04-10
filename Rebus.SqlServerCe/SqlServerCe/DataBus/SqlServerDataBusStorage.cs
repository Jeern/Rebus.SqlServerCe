﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.DataBus;
using Rebus.Exceptions;
using Rebus.Logging;
using Rebus.Serialization;
using Rebus.SqlServerCe.Extensions;
using Rebus.Time;

namespace Rebus.SqlServerCe.DataBus
{
    /// <summary>
    /// Implementation of <see cref="IDataBusStorage"/> that uses SQL Server Compact to store data
    /// </summary>
    public class SqlServerCeDataBusStorage : IDataBusStorage, IInitializable
    {
        static readonly Encoding TextEncoding = Encoding.UTF8;
        readonly DictionarySerializer _dictionarySerializer = new DictionarySerializer();
        readonly IDbConnectionProvider _connectionProvider;
        readonly TableName _tableName;
        readonly bool _ensureTableIsCreated;
        readonly ILog _log;

        /// <summary>
        /// Creates the data storage
        /// </summary>
        public SqlServerCeDataBusStorage(IDbConnectionProvider connectionProvider, string tableName, bool ensureTableIsCreated, IRebusLoggerFactory rebusLoggerFactory)
        {
            if (connectionProvider == null) throw new ArgumentNullException(nameof(connectionProvider));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            _connectionProvider = connectionProvider;
            _tableName = new TableName(tableName);
            _ensureTableIsCreated = ensureTableIsCreated;
            _log = rebusLoggerFactory.GetLogger<SqlServerCeDataBusStorage>();
        }

        /// <summary>
        /// Initializes the SQL Server Compact data storage.
        /// Will create the data table, unless this has been explicitly turned off when configuring the data storage
        /// </summary>
        public void Initialize()
        {
            if (!_ensureTableIsCreated) return;

            _log.Info("Creating data bus table {tableName}", _tableName.Name);

            EnsureTableIsCreated().Wait();
        }

        async Task EnsureTableIsCreated()
        {
            using (var connection = await _connectionProvider.GetConnection())
            {
                if (connection.GetTableNames().Contains(_tableName))
                    return;

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
    CREATE TABLE {_tableName.Name} (
        [Id] NVARCHAR(200),
        [Meta] NTEXT,
        [Data] NTEXT,
        [LastReadTime] DATETIME
    );

";
                    if (!command.TryExecute())
                        return; // table already exists - just quit now
                }

                await connection.Complete();
            }
        }

        /// <summary>
        /// Saves the data from the given source stream under the given ID
        /// </summary>
        public async Task Save(string id, Stream source, Dictionary<string, string> metadata = null)
        {
            var metadataToWrite = new Dictionary<string, string>(metadata ?? new Dictionary<string, string>())
            {
                [MetadataKeys.SaveTime] = RebusTime.Now.ToString("O")
            };

            try
            {
                using (var connection = await _connectionProvider.GetConnection())
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = $"INSERT INTO {_tableName.Name} ([Id], [Meta], [Data]) VALUES (@id, @meta, @data)";
                        command.Parameters.Add("id", SqlDbType.NVarChar, 200).Value = id;
                        command.Parameters.Add("meta", SqlDbType.NText).Value = TextEncoding.GetBytes(_dictionarySerializer.SerializeToString(metadataToWrite));
                        command.Parameters.Add("data", SqlDbType.NText).Value = source;

                        await command.ExecuteNonQueryAsync();
                    }

                    await connection.Complete();
                }
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not save data with ID {id}");
            }
        }

        /// <summary>
        /// Opens the data stored under the given ID for reading
        /// </summary>
        public async Task<Stream> Read(string id)
        {
            try
            {
                // update last read time quickly
                await UpdateLastReadTime(id);

                var connection = await _connectionProvider.GetConnection();

                using (var command = connection.CreateCommand())
                {
                    try
                    {
                        command.CommandText = $"SELECT TOP 1 [Data] FROM {_tableName.Name} WITH (NOLOCK) WHERE [Id] = @id";
                        command.Parameters.Add("id", SqlDbType.NVarChar, 200).Value = id;

                        var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess);

                        if (!await reader.ReadAsync())
                        {
                            throw new ArgumentException($"Row with ID {id} not found");
                        }

                        var dataOrdinal = reader.GetOrdinal("data");
                        var stream = reader.GetStream(dataOrdinal);

                        return new StreamWrapper(stream, new IDisposable[]
                        {
                            // defer closing these until the returned stream is closed
                            reader,
                            connection
                        });
                    }
                    catch
                    {
                        // if something of the above fails, we did not pass the connection to someone who can dispose it... wherefore:
                        connection.Dispose();
                        throw;
                    }
                }
            }
            catch (ArgumentException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not load data with ID {id}");
            }
        }

        async Task UpdateLastReadTime(string id)
        {
            using (var connection = await _connectionProvider.GetConnection())
            {
                await UpdateLastReadTime(id, connection);
                await connection.Complete();
            }
        }

        async Task UpdateLastReadTime(string id, IDbConnection connection)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"UPDATE {_tableName.Name} SET [LastReadTime] = @now WHERE [Id] = @id";
                command.Parameters.Add("now", SqlDbType.DateTime).Value = RebusTime.Now;
                command.Parameters.Add("id", SqlDbType.NVarChar, 200).Value = id;
                await command.ExecuteNonQueryAsync();
            }
        }

        /// <summary>
        /// Loads the metadata stored with the given ID
        /// </summary>
        public async Task<Dictionary<string, string>> ReadMetadata(string id)
        {
            try
            {
                using (var connection = await _connectionProvider.GetConnection())
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = $"SELECT TOP 1 [Meta], [LastReadTime], DATALENGTH([Data]) AS 'Length' FROM {_tableName.Name} WITH (NOLOCK) WHERE [Id] = @id";
                        command.Parameters.Add("id", SqlDbType.NVarChar, 200).Value = id;

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            if (!await reader.ReadAsync())
                            {
                                throw new ArgumentException($"Row with ID {id} not found");
                            }

                            var bytes = (byte[])reader["Meta"];
                            var length = (long)reader["Length"];
                            var lastReadTimeDbValue = reader["LastReadTime"];

                            var jsonText = TextEncoding.GetString(bytes);
                            var metadata = _dictionarySerializer.DeserializeFromString(jsonText);

                            metadata[MetadataKeys.Length] = length.ToString();

                            if (lastReadTimeDbValue != DBNull.Value)
                            {
                                var lastReadTime = (DateTime)lastReadTimeDbValue;

                                metadata[MetadataKeys.ReadTime] = lastReadTime.ToString("O");
                            }

                            return metadata;
                        }
                    }
                }
            }
            catch (ArgumentException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not load metadata for data with ID {id}");
            }
        }
    }
}