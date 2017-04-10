using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data.SqlServerCe;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.SqlServerCe.Extensions
{
    /// <summary>
    /// Extensions for SqlCeCommand objects
    /// </summary>
    public static class CommandExtensions
    {
        /// <summary>
        /// This is ExecuteNonQuery wrapped in a try / catch to mitigate that Sql Server Compact does not support "IF NOT EXISTS"
        /// See http://stackoverflow.com/questions/4652867/if-not-exists-fails-on-sql-ce
        /// </summary>
        /// <param name="command"></param>
        public static bool TryExecute(this SqlCeCommand command)
        {
            const int tableAlreadyExists = 2714;
            try
            {
                command.ExecuteNonQuery();
            }
            catch (SqlCeException exception) when (exception.NativeError == tableAlreadyExists)
            {
                // table already exists
                return false;
            }
            catch (SqlCeException exception)
            {
                Console.WriteLine(exception.NativeError);
                return false;
            }
            return true;
        }

        /// <summary>
        /// This is ExecuteNonQueryAsync wrapped in a try / catch to mitigate that Sql Server Compact does not support "IF NOT EXISTS"
        /// See http://stackoverflow.com/questions/4652867/if-not-exists-fails-on-sql-ce
        /// </summary>
        /// <param name="command"></param>
        /// <returns></returns>
        public static async Task<bool> TryExecuteAsync(this SqlCeCommand command)
        {
            const int tableAlreadyExists = 2714;
            try
            {
                await command.ExecuteNonQueryAsync();
            }
            catch (SqlCeException exception) when (exception.NativeError == tableAlreadyExists)
            {
                // table already exists
                return false;
            }
            catch (SqlCeException exception) 
            {
                Console.WriteLine(exception.NativeError);
                return false;
            }
            return true;
        }
    }
}
