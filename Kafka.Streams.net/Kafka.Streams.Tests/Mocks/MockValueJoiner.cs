using Kafka.Streams.KStream;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Tests.Mocks
{
    public static class MockValueJoiner
    {
        private class ToStringJoiner<V1, V2, VR> : IValueJoiner<V1, V2, string>
        {
            private readonly string separator;

            public ToStringJoiner(string separator)
            {
                this.separator = separator;
            }

            public string apply(V1 value1, V2 value2)
            {
                return $"{value1}{this.separator}{value2}";
            }

            public string apply(V1 value1)
            {
                throw new NotImplementedException();
            }
        }

        public static IValueJoiner<V1, V2, string> TOSTRING_JOINER<V1, V2>() => instance<V1, V2>("+");

        public static IValueJoiner<V1, V2, string> instance<V1, V2>(string separator)
        {
            return new ToStringJoiner<V1, V2, string>(separator);
        }
    }
}
