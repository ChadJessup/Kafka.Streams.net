using NodaTime;

namespace Kafka.Streams.KStream
{
    public abstract class Window
    {
        public Interval Interval { get; }

        /**
         * Create a new window for the given start and end time.
         *
         * @param startMs the start timestamp of the window
         * @param endMs   the end timestamp of the window
         * @throws ArgumentException if {@code startMs} is negative or if {@code endMs} is smaller than {@code startMs}
         */
        public Window(Instant startTime, Instant endTime)
        {
            this.Interval = new Interval(startTime, endTime);
        }

        public Window(Duration duration)
            : this(SystemClock.Instance.GetCurrentInstant(), duration)
        {
        }

        public Window(Instant startTime, Duration duration)
            : this(startTime, duration == Duration.MaxValue ? Instant.FromUnixTimeMilliseconds((long)duration.TotalMilliseconds - startTime.ToUnixTimeMilliseconds()) : startTime + duration)
        {
        }

        /**
         * Return the start timestamp of this window.
         *
         * @return The start timestamp of this window.
         */
        public long Start()
        {
            return this.Interval.Start.ToUnixTimeMilliseconds();
        }

        /**
         * Return the end timestamp of this window.
         *
         * @return The end timestamp of this window.
         */
        public long End()
        {
            return this.Interval.End.ToUnixTimeMilliseconds();
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

            var other = (Window)obj;
            return this.Start() == other.Start() && this.End() == other.End();
        }

        public override int GetHashCode()
        {
            return this.Interval.GetHashCode();
        }

        public override string ToString()
        {
            return "Window{" +
                $"startMs={this.Interval.Start.ToUnixTimeMilliseconds()}" +
                $", endMs={this.Interval.End.ToUnixTimeMilliseconds()}" +
                '}';
        }
    }
}
