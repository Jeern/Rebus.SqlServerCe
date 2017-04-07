using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Rebus.SqlServerCe
{
    /// <summary>
    /// SQL Server Compact database connection provider that allows for easily changing how the current <see cref="SqlConnection"/> is obtained,
    /// possibly also changing how transactions are handled
    /// </summary>
    public interface IDbConnectionProvider
    {
        /// <summary>
        /// Gets a wrapper with the current <see cref="SqlConnection"/> inside
        /// </summary>
        Task<IDbConnection> GetConnection();
    }
}