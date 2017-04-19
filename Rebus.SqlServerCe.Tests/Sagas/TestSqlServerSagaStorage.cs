using System;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.SqlServerCe.Extensions;
using Rebus.SqlServerCe.Sagas;
using Rebus.Tests.Contracts;

namespace Rebus.SqlServerCe.Tests.Sagas
{
    [TestFixture]
    public class TestSqlServerCeSagaStorage : FixtureBase
    {
        SqlServerCeSagaStorage _storage;
        string _dataTableName;
        DbConnectionProvider _connectionProvider;
        string _indexTableName;

        protected override void SetUp()
        {
            var loggerFactory = new ConsoleLoggerFactory(false);
            _connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, loggerFactory);

            _dataTableName = TestConfig.GetName("sagas");
            _indexTableName = TestConfig.GetName("sagaindex");

            SqlTestHelper.DropTable(_indexTableName);
            SqlTestHelper.DropTable(_dataTableName);

            _storage = new SqlServerCeSagaStorage(_connectionProvider, _dataTableName, _indexTableName, loggerFactory);
        }

        [Test]
        public async Task DoesNotThrowExceptionWhenInitializeOnOldSchema()
        {
            await CreatePreviousSchema();

            _storage.Initialize();

            _storage.EnsureTablesAreCreated();
        }

        [Test]
        public async Task CanRoundtripSagaOnOldSchema()
        {
            var noProps = Enumerable.Empty<ISagaCorrelationProperty>();

            await CreatePreviousSchema();

            _storage.Initialize();

            var sagaData = new MySagaDizzle {Id=Guid.NewGuid(), Text = "whee!"};

            await _storage.Insert(sagaData, noProps);

            var roundtrippedData = await _storage.Find(typeof(MySagaDizzle), "Id", sagaData.Id.ToString());

            Assert.That(roundtrippedData, Is.TypeOf<MySagaDizzle>());
            var sagaData2 = (MySagaDizzle)roundtrippedData;
            Assert.That(sagaData2.Text, Is.EqualTo(sagaData.Text));
        }

        class MySagaDizzle : ISagaData
        {
            public Guid Id { get; set; }
            public int Revision { get; set; }
            public string Text { get; set; }
        }

        async Task CreatePreviousSchema()
        {
            var createTableOldSchema =
                $@"
CREATE TABLE [{_dataTableName}](
	[id] [uniqueidentifier] NOT NULL,
	[revision] [int] NOT NULL,
	[data] [image] NOT NULL
)

----

ALTER TABLE {_dataTableName} ADD CONSTRAINT [PK_{_dataTableName}] PRIMARY KEY ([id])

";

            var createTableOldSchema2 =
                $@"
CREATE TABLE [{_indexTableName}](
	[saga_type] [nvarchar](40) NOT NULL,
	[key] [nvarchar](200) NOT NULL,
	[value] [nvarchar](200) NOT NULL,
	[saga_id] [uniqueidentifier] NOT NULL
)

----

CREATE UNIQUE INDEX [PK_{_indexTableName}] ON {_indexTableName} ([key], [value], [saga_type])
";

            Console.WriteLine($"Creating tables {_dataTableName} and {_indexTableName}");

            using (var connection = await _connectionProvider.GetConnection())
            {
                connection.TryExecuteCommands(createTableOldSchema);
                connection.TryExecuteCommands(createTableOldSchema2);

                await connection.Complete();
            }
        }
    }
}