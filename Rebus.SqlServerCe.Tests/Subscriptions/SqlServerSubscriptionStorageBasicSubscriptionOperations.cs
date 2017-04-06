using NUnit.Framework;
using Rebus.Tests.Contracts.Subscriptions;

namespace Rebus.SqlServerCe.Tests.Subscriptions
{
    [TestFixture, Category(Categories.SqlServer)]
    public class SqlServerSubscriptionStorageBasicSubscriptionOperations : BasicSubscriptionOperations<SqlServerSubscriptionStorageFactory>
    {
    }
}