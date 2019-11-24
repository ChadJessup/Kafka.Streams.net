using NodaTime;
using System;

namespace Kafka.Streams.KStream
{
    public abstract class Window
    {
        protected Duration Duration { get; }
        private readonly Instant startTime;
        private readonly Instant endTime;

        /**
         * Create a new window for the given start and end time.
         *
         * @param startMs the start timestamp of the window
         * @param endMs   the end timestamp of the window
         * @throws ArgumentException if {@code startMs} is negative or if {@code endMs} is smaller than {@code startMs}
         */
        public Window(Duration duration)
        {
            if (duration.TotalNanoseconds < 0)
            {
                throw new ArgumentException("Window duration cannot be negative.");
            }

            this.startTime = SystemClock.Instance.GetCurrentInstant();
            this.endTime = this.startTime + duration;
        }

        /**
         * Return the start timestamp of this window.
         *
         * @return The start timestamp of this window.
         */
        public long Start()
        {
            return this.startTime.ToUnixTimeMilliseconds();
        }

        /**
         * Return the end timestamp of this window.
         *
         * @return The end timestamp of this window.
         */
        public long End()
        {
            return this.endTime.ToUnixTimeMilliseconds();
        }

        /**
         * Check if the given window overlaps with this window.
         * Should throw an {@link ArgumentException} if the {@code other} window has a different type than {@code
         * this} window.
         *
         * @param other another window of the same type
         * @return {@code true} if {@code other} overlaps with this window&mdash;{@code false} otherwise
         */
        public abstract bool Overlap(Window other);

        public override bool Equals(object obj)
        {
            if (obj == this)
            {
                return true;
            }
            if (obj == null)
            {
                return false;
            }
            if (GetType() != obj.GetType())
            {
                return false;
            }

            Window other = (Window)obj;
            return startMs == other.startMs && endMs == other.endMs;
        }

        public override int GetHashCode()
        {
            return (int)(((startMs << 32) | endMs) % 0xFFFFFFFFL);
        }

        public override string ToString()
        {
            return "Window{" +
                "startMs=" + startMs +
                ", endMs=" + endMs +
                '}';
        }
    }
}
