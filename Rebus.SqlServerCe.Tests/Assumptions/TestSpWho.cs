using System;
using System.Linq;
using NUnit.Framework;

namespace Rebus.SqlServerCe.Tests.Assumptions
{
    [TestFixture]
    public class TestSpWho
    {
        [Test]
        public void DropTableThatDoesNotExist()
        {
            SqlTestHelper.DropTable("bimse");
        }
    }
}