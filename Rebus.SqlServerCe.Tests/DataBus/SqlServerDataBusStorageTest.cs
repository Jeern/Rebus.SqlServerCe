using NUnit.Framework;
using Rebus.Tests.Contracts.DataBus;

namespace Rebus.SqlServerCe.Tests.DataBus
{
    [TestFixture]
    public class SqlServerCeDataBusStorageTest : GeneralDataBusStorageTests<SqlServerCeDataBusStorageFactory> { }
}