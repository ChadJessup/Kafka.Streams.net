using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Tests.Mocks
{
    public static class MockAggregator<K, V>
    {
        public static Aggregator<K, V, string> TOSTRING_ADDER = ToStringInstance("+");
        public static Aggregator<K, V, string> TOSTRING_REMOVER = ToStringInstance("-");

        public static Aggregator<K, V, string> ToStringInstance(string sep)
            => (aggKey, value, aggregate) => aggregate + sep + value;
    }
}
