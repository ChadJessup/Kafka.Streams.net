using System;

namespace Kafka.Streams.KStream.Internals
{
    public class SessionWindow : Window
    {
        public SessionWindow(TimeSpan duration)
            : base(duration)
        {
        }

        public SessionWindow(DateTime startTime, DateTime duration)
            : base(startTime, duration)
        {
        }

        public SessionWindow(long start, long end)
            : base(start, end)
        {
        }

        public override bool Overlap(Window other)
        {
            if (this.GetType() != other.GetType())
            {
                throw new System.ArgumentException("Cannot compare windows of different type. Other window has type "
                    + other.GetType() + ".");
            }

            SessionWindow otherWindow = (SessionWindow)other;

            return !(otherWindow.End() < this.Start() || this.End() < otherWindow.Start());
        }
    }
}
