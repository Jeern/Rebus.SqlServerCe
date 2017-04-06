using NUnit.Framework;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.SqlServerCe.Tests.Sagas
{
    [TestFixture]
    public class TestSagaCorrelationSql : TestSagaCorrelation<SqlServerSagaStorageFactory> { }
}