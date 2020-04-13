using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Tests.Mocks
{
    public static class MockAggregator
    {
        public static IAggregator<object, object, string> TOSTRING_ADDER = ToStringInstance<object, object>("+");
        public static IAggregator<object, object, string> TOSTRING_REMOVER = ToStringInstance<object, object>("-");

        public static IAggregator<K, V, string> ToStringInstance<K, V>(string sep)
        {
            return new Aggregator<K, V, string>((aggKey, value, aggregate) => aggregate + sep + value);
        }
    }
}
