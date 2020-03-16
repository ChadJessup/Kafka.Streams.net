using NodaTime;

namespace Kafka.Streams.KStream.Internals
{
    public class SessionWindow : Window
    {
        public SessionWindow(Duration duration)
            : base(duration)
        {
        }

        public SessionWindow(Instant startTime, Duration duration)
            : base(startTime, duration)
        {
        }

        public SessionWindow(long start, long end)
            : base(Instant.FromUnixTimeMilliseconds(start), Instant.FromUnixTimeMilliseconds(end))
        {
        }

        public override bool Overlap(Window other)
        {
            if (GetType() != other.GetType())
            {
                throw new System.ArgumentException("Cannot compare windows of different type. Other window has type "
                    + other.GetType() + ".");
            }

            SessionWindow otherWindow = (SessionWindow)other;

            return !(otherWindow.Start() < this.Start() || this.End() < otherWindow.Start());
        }
    }
}
