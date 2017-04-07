using System.Data.SqlClient;
using System.Data.SqlServerCe;
using System.Threading.Tasks;

namespace Rebus.SqlServerCe
{
    /// <summary>
    /// SQL Server Compact database connection provider that allows for easily changing how the current <see cref="SqlCeConnection"/> is obtained,
    /// possibly also changing how transactions are handled
    /// </summary>
    public interface IDbConnectionProvider
    {
        /// <summary>
        /// Gets a wrapper with the current <see cref="SqlCeConnection"/> inside
        /// </summary>
        Task<IDbConnection> GetConnection();
    }
}