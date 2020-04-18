using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Tests.Mocks
{
    public static class MockValueJoiner
    {
        public static ValueJoiner<string, string, string> TOSTRING_JOINER()
            => Instance("+");

        public static ValueJoiner<string, string, string> Instance(string separator)
            => (v1, v2) => $"{v1}{separator}{v2}";
    }
}
