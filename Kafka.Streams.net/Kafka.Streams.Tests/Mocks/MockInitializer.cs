using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Tests.Mocks
{
    public static class MockInitializer
    {
        private class StringInit : IInitializer<string>
        {
            public string Apply()
            {
                return "0";
            }
        }

        public static IInitializer<string> STRING_INIT = new StringInit();
    }
}
