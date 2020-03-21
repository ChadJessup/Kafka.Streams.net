using System;

namespace Kafka.Streams.Tests.Mocks
{
    public class MockPredicate
    {
        public static Func<K, V, bool> AllGoodPredicate<K, V>()
        {
            return new Func<K, V, bool>((k, v) => true);
        }
    }
}
