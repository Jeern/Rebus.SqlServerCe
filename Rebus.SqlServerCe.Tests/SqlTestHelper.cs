using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data.SqlServerCe;
using System.IO;
using System.Linq;
using Rebus.Exceptions;
using Rebus.Tests.Contracts;

namespace Rebus.SqlServerCe.Tests
{
    public class SqlTestHelper
    {
        static string _connectionString;

        public static string ConnectionString
        {
            get
            {
                if (_connectionString != null)
                    return _connectionString;

                InitializeDatabase();

                Console.WriteLine("Using SQL Compact database {0}", DatabaseFile);

                _connectionString = GetConnectionStringForDatabase(DatabaseFile);

                return _connectionString;
            }
        }


        public static void Execute(string sql)
        {
            using (var connection = new SqlCeConnection(ConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sql;
                    command.ExecuteNonQuery();
                }
            }
        }

        public static void DropTable(string tableName)
        {
            var table = TableName.Parse(tableName);
            DropObject($"DROP TABLE {table.Name}", connection =>
            {
                var tableNames = connection.GetTableNames();

                return tableNames.Contains(table);
            });
        }

        public static void DropIndex(string tableName, string indexName)
        {
            DropObject($"DROP INDEX [{indexName}] ON [{tableName}]", connection =>
            {
                var indexNames = connection.GetIndexNames();

                return indexNames.Contains(indexName, StringComparer.OrdinalIgnoreCase);
            });
        }

        static void DropObject(string sqlCommand, Func<SqlCeConnection, bool> executeCriteria)
        {
            try
            {
                using (var connection = new SqlCeConnection(ConnectionString))
                {
                    connection.Open();

                    var shouldExecute = executeCriteria(connection);
                    if (!shouldExecute) return;

                    try
                    {
                        using (var command = connection.CreateCommand())
                        {
                            command.CommandText = sqlCommand;
                            command.ExecuteNonQuery();
                        }
                    }
                    catch (SqlException exception)
                    {
                        if (exception.Number == SqlServerCeMagic.ObjectDoesNotExistOrNoPermission) return;

                        throw;
                    }
                }
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not execute '{sqlCommand}'");
            }
        }



        static void CreateDatabaseFile(string fileName, string connectionString)
        {
            if (!File.Exists(fileName))
            {
                var engine = new SqlCeEngine(connectionString);
                engine.CreateDatabase();
            }
        }

        static void InitializeDatabase()
        {
            try
            {
                CreateDatabaseDirectory(DatabaseDirectory);
                CreateDatabaseFile(DatabaseFile, GetConnectionStringForDatabase(DatabaseFile));
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not initialize database '{DatabaseFile}'");
            }
        }

        private static void CreateDatabaseDirectory(string databaseDirectory)
        {
            if (!Directory.Exists(databaseDirectory))
            {
                Directory.CreateDirectory(databaseDirectory);
            }
        }

        static string GetConnectionStringForDatabase(string fileName)
        {
            return Environment.GetEnvironmentVariable("REBUS_SqlServerCe")
                   ?? $"Data Source='{fileName}'";
        }

        static string DatabaseDirectory => Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Database");
        static string DatabaseFile => Path.Combine(DatabaseDirectory, FileName);
        static string FileName => $"rebus2_test_{TestConfig.Suffix}".TrimEnd('_') + ".sdf";
    }
}