using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.SqlServerCe.Transport;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Transports;

namespace Rebus.SqlServerCe.Tests
{
    public class SqlServerCeBusFactory : IBusFactory
    {
        readonly List<IDisposable> _stuffToDispose = new List<IDisposable>();

        public IBus GetBus<TMessage>(string inputQueueAddress, Func<TMessage, Task> handler)
        {
            var builtinHandlerActivator = new BuiltinHandlerActivator();

            builtinHandlerActivator.Handle(handler);

            var tableName = "messages" + TestConfig.Suffix;

            SqlTestHelper.DropTable(tableName);

            var bus = Configure.With(builtinHandlerActivator)
                .Transport(t => t.UseSqlServerCe(SqlTestHelper.ConnectionString, tableName, inputQueueAddress))
                .Options(o =>
                {
                    o.SetNumberOfWorkers(10);
                    o.SetMaxParallelism(10);
                })
                .Start();

            _stuffToDispose.Add(bus);

            return bus;
        }

        public void Cleanup()
        {
            _stuffToDispose.ForEach(d => d.Dispose());
            _stuffToDispose.Clear();
        }
    }
}