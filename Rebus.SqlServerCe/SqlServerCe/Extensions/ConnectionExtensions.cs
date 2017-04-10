using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rebus.Exceptions;

namespace Rebus.SqlServerCe.Extensions
{
    /// <summary>
    /// Extensions for IDbConnection
    /// </summary>
    public static class ConnectionExtensions
    {
        /// <summary>
        /// Splits the sqlCommands string into multiple commands  (delimited by "----") and Tries to execute each one
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="sqlCommands"></param>
        public static void TryExecuteCommands(this IDbConnection connection, string sqlCommands)
        {
            foreach (var sqlCommand in sqlCommands.Split(new[] { "----" }, StringSplitOptions.RemoveEmptyEntries))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sqlCommand;
                    command.TryExecute();
                }
            }
        }

        /// <summary>
        /// Splits the sqlCommands string into multiple commands (delimited by "----") and Tries to execute each one
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="sqlCommands"></param>
        public static async Task TryExecuteCommandsAsync(this IDbConnection connection, string sqlCommands)
        {
            foreach (var commandText in sqlCommands.Split(new[] { "----" }, StringSplitOptions.RemoveEmptyEntries))
            {
                try
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = commandText;
                        await command.TryExecuteAsync();
                    }
                }
                catch (Exception exception)
                {
                    throw new RebusApplicationException(exception, $@"Could not execute SQL:

{commandText}");
                }
            }

        }


    }
}
