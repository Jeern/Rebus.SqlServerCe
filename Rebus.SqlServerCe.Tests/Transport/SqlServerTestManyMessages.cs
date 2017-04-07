using NUnit.Framework;
using Rebus.Tests.Contracts.Transports;

namespace Rebus.SqlServerCe.Tests.Transport
{
    [TestFixture]
    public class SqlServerCeTestManyMessages : TestManyMessages<SqlServerCeBusFactory> { }
}