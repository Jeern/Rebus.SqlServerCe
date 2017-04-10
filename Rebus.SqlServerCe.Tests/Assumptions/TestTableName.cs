using System;
using System.Text.RegularExpressions;
using NUnit.Framework;

namespace Rebus.SqlServerCe.Tests.Assumptions
{
    [TestFixture]
    public class TestTableName
    {
        [TestCase("[bimse]", "[bimse]", true)]
        [TestCase("[bimse]", "[BIMSE]", true)]
        public void CheckEquality(string name1, string name2, bool expectedToBeEqual)
        {
            var tableName1 = new TableName(name1);
            var tableName2 = new TableName(name2);

            var what = expectedToBeEqual
                ? Is.EqualTo(tableName2)
                : Is.Not.EqualTo(tableName2);

            Assert.That(tableName1, what);
        }

        [TestCase("table].[schema")]
        [TestCase("table] .[schema")]
        [TestCase("table]  .[schema")]
        [TestCase("table]. [schema")]
        [TestCase("table].  [schema")]
        [TestCase("table].   [schema")]
        [TestCase("table] . [schema")]
        public void RegexSplitter(string text)
        {
            var partsThingie = Regex.Split(text, @"\][ ]*\.[ ]*\[");

            Console.WriteLine($"Found parts: {string.Join(", ", partsThingie)}");
        }
    }
}
