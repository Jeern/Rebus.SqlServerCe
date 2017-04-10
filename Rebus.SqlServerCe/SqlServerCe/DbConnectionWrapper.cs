﻿using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data.SqlServerCe;
using System.Linq;
using System.Threading.Tasks;
using Rebus.Exceptions;

#pragma warning disable 1998

namespace Rebus.SqlServerCe
{
    /// <summary>
    /// Wrapper of <see cref="SqlCeConnection"/> that allows for either handling <see cref="SqlCeTransaction"/> automatically, or for handling it externally
    /// </summary>
    public class DbConnectionWrapper : IDbConnection
    {
        readonly SqlCeConnection _connection;
        readonly bool _managedExternally;

        SqlCeTransaction _currentTransaction;
        bool _disposed;

        /// <summary>
        /// Constructs the wrapper, wrapping the given connection and transaction. It must be indicated with <paramref name="managedExternally"/> whether this wrapper
        /// should commit/rollback the transaction (depending on whether <see cref="Complete"/> is called before <see cref="Dispose()"/>), or if the transaction
        /// is handled outside of the wrapper
        /// </summary>
        public DbConnectionWrapper(SqlCeConnection connection, SqlCeTransaction currentTransaction, bool managedExternally)
        {
            _connection = connection;
            _currentTransaction = currentTransaction;
            _managedExternally = managedExternally;
        }

        /// <summary>
        /// Creates a ready to used <see cref="SqlCeCommand"/>
        /// </summary>
        public SqlCeCommand CreateCommand()
        {
            var sqlCommand = _connection.CreateCommand();
            sqlCommand.Transaction = _currentTransaction;
            return sqlCommand;
        }

        /// <summary>
        /// Gets the names of all the tables in the current database 
        /// </summary>
        public IEnumerable<TableName> GetTableNames()
        {
            try
            {
                return _connection.GetTableNames(_currentTransaction);
            }
            catch (SqlCeException exception)
            {
                throw new RebusApplicationException(exception, "Could not get table names");
            }
        }

        /// <summary>
        /// Gets information about the columns in the table given by <paramref name="dataTableName"/>
        /// </summary>
        public IEnumerable<DbColumn> GetColumns(string dataTableName)
        {
            try
            {
                return _connection
                    .GetColumns(dataTableName, _currentTransaction)
                    .Select(kvp => new DbColumn(kvp.Key, kvp.Value))
                    .ToList();
            }
            catch (SqlCeException exception)
            {
                throw new RebusApplicationException(exception, "Could not get table names");
            }
        }

        /// <summary>
        /// Marks that all work has been successfully done and the <see cref="SqlCeConnection"/> may have its transaction committed or whatever is natural to do at this time
        /// </summary>
        public async Task Complete()
        {
            if (_managedExternally) return;

            if (_currentTransaction != null)
            {
                using (_currentTransaction)
                {
                    _currentTransaction.Commit();
                    _currentTransaction = null;
                }
            }
        }

        /// <summary>
        /// Finishes the transaction and disposes the connection in order to return it to the connection pool. If the transaction
        /// has not been committed (by calling <see cref="Complete"/>), the transaction will be rolled back.
        /// </summary>
        public void Dispose()
        {
            if (_managedExternally) return;
            if (_disposed) return;

            try
            {
                try
                {
                    if (_currentTransaction != null)
                    {
                        using (_currentTransaction)
                        {
                            try
                            {
                                _currentTransaction.Rollback();
                            }
                            catch { }
                            _currentTransaction = null;
                        }
                    }
                }
                finally
                {
                    _connection.Dispose();
                }
            }
            finally
            {
                _disposed = true;
            }
        }
    }
}