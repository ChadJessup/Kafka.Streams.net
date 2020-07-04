using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Tests.Mocks
{
    public static class MockInitializer
    {
        private class StringInit : IInitializer2<string>
        {
            public string Apply()
            {
                return "0";
            }
        }

        public static Initializer<string> STRING_INIT = () => "0";
    }
}
