using Kafka.Streams.KStream;

namespace Kafka.Streams.Tests.Mocks
{
    public static class MockReducer
    {
        public static Reducer<string> StringAdd = (v1, v2) => $"{v1}+{v2}";
        public static Reducer<string> StringRemove = (v1, v2) => $"{v1}-{v2}";
        public static Reducer<int> IntergerAdd = (v1, v2) => v1 + v2;
        public static Reducer<int> IntegerSubtract = (v1, v2) => v1 - v2;
    }
}
