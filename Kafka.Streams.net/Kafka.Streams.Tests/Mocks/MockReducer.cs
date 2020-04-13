using Kafka.Streams.KStream;

namespace Kafka.Streams.Tests.Mocks
{
    public static class MockReducer
    {
        private class StringAdd : IReducer<string>
        {
            public string Apply(string value1, string value2)
            {
                return value1 + "+" + value2;
            }
        }

        private class StringRemove : IReducer<string>
        {
            public string Apply(string value1, string value2)
            {
                return value1 + "-" + value2;
            }
        }

        private class IntegerAdd : IReducer<int>
        {
            public int Apply(int value1, int value2)
            {
                return value1 + value2;
            }
        }

        private class IntegerSubtract : IReducer<int>
        {
            public int Apply(int value1, int value2)
            {
                return value1 - value2;
            }
        }

        public static IReducer<string> STRING_ADDER = new StringAdd();
        public static IReducer<string> STRING_REMOVER = new StringRemove();
        public static IReducer<int> INTEGER_ADDER = new IntegerAdd();
        public static IReducer<int> INTEGER_SUBTRACTOR = new IntegerSubtract();
    }
}
