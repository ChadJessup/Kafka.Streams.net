using Kafka.Streams.KStream.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Tests.Mocks
{
    public class MockPredicate
    {

        private class AlwaysGoodPredicate<K, V> : IPredicate<K, V>
        {
            public Func<K, V, bool> test(K key, V value)
            {
                return (k, v) => true;
            }
        }

        public static IPredicate<K, V> AllGoodPredicate<K, V>()
        {
            return new AlwaysGoodPredicate<K, V>();
        }
    }

}
