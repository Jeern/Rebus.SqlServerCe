using NUnit.Framework;
using Rebus.Tests.Contracts.Subscriptions;

namespace Rebus.SqlServerCe.Tests.Subscriptions
{
    [TestFixture, Category(Categories.SqlServerCe)]
    public class SqlServerCeSubscriptionStorageBasicSubscriptionOperations : BasicSubscriptionOperations<SqlServerCeSubscriptionStorageFactory>
    {
    }
}