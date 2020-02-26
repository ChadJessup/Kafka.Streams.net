using NodaTime;

namespace Kafka.Streams.Tests
{
    public class MockTime : IClock
    {
        public MockTime()
        {
        }

        public Instant GetCurrentInstant()
        {
            return Instant.MinValue;
        }
    }
}