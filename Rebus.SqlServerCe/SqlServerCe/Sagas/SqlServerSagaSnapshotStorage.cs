using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Rebus.Auditing.Sagas;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.Serialization;
using Rebus.SqlServerCe.Extensions;

namespace Rebus.SqlServerCe.Sagas
{
    /// <summary>
    /// Implementation of <see cref="ISagaSnapshotStorage"/> that uses a table in SQL Server Compact to store saga snapshots
    /// </summary>
    public class SqlServerCeSagaSnapshotStorage : ISagaSnapshotStorage
    {
        readonly IDbConnectionProvider _connectionProvider;
        readonly TableName _tableName;
        readonly ILog _log;

        static readonly ObjectSerializer DataSerializer = new ObjectSerializer();
        static readonly HeaderSerializer MetadataSerializer = new HeaderSerializer();

        /// <summary>
        /// Constructs the snapshot storage
        /// </summary>
        public SqlServerCeSagaSnapshotStorage(IDbConnectionProvider connectionProvider, string tableName, IRebusLoggerFactory rebusLoggerFactory)
        {
            if (connectionProvider == null) throw new ArgumentNullException(nameof(connectionProvider));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));

            _log = rebusLoggerFactory.GetLogger<SqlServerCeSagaSnapshotStorage>();
            _connectionProvider = connectionProvider;
            _tableName = new TableName(tableName);
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

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
    CREATE TABLE {_tableName.Name} (
	    [id] [uniqueidentifier] NOT NULL,
	    [revision] [int] NOT NULL,
	    [data] [ntext] NOT NULL,
	    [metadata] [ntext] NOT NULL,
        CONSTRAINT [PK_{_tableName.Name}] PRIMARY KEY CLUSTERED 
        (
	        [id] ASC,
            [revision] ASC
        )
    )

";
                    command.TryExecute();
                }

                connection.Complete();
            }
        }

        /// <summary>
        /// Saves a snapshot of the saga data along with the given metadata
        /// </summary>
        public async Task Save(ISagaData sagaData, Dictionary<string, string> sagaAuditMetadata)
        {
            using (var connection = await _connectionProvider.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"

INSERT INTO {_tableName.Name} (
    [id],
    [revision],
    [data],
    [metadata]
) VALUES (
    @id, 
    @revision, 
    @data,
    @metadata
)

";
                    command.Parameters.Add("id", SqlDbType.UniqueIdentifier).Value = sagaData.Id;
                    command.Parameters.Add("revision", SqlDbType.Int).Value = sagaData.Revision;
                    command.Parameters.Add("data", SqlDbType.NVarChar).Value = DataSerializer.SerializeToString(sagaData);
                    command.Parameters.Add("metadata", SqlDbType.NVarChar).Value = MetadataSerializer.SerializeToString(sagaAuditMetadata);

                    await command.ExecuteNonQueryAsync();
                }

                await connection.Complete();
            }
        }
    }
}