using Confluent.Kafka;
using System;

namespace Kafka.Streams.KStream
{
    public abstract class Window
    {
        public TimeSpan Interval { get; }
        public DateTime StartTime { get; }
        public DateTime EndTime { get; }

        /**
         * Create a new window for the given start and end time.
         *
         * @param startMs the start timestamp of the window
         * @param endMs   the end timestamp of the window
         * @throws ArgumentException if {@code startMs} is negative or if {@code endMs} is smaller than {@code startMs}
         */
        public Window(DateTime startTime, DateTime endTime)
        {
            this.Interval = endTime - startTime;
            this.StartTime = startTime;
            this.EndTime = endTime;
        }

        public Window(long startMs, long endMs)
            : this(Timestamp.UnixTimestampMsToDateTime(startMs), Timestamp.UnixTimestampMsToDateTime(endMs))
        {
        }

        public Window(TimeSpan duration)
            : this(DateTime.UtcNow, duration)
        {
        }

        public Window(DateTime startTime, TimeSpan duration)
            : this(startTime, duration == TimeSpan.MaxValue ? DateTime.MaxValue : startTime + duration)
        {
        }

        /**
         * Return the start timestamp of this window.
         *
         * @return The start timestamp of this window.
         */
        public long Start()
            => Timestamp.DateTimeToUnixTimestampMs(this.StartTime);

        /**
         * Return the end timestamp of this window.
         *
         * @return The end timestamp of this window.
         */
        public long End()
            => Timestamp.DateTimeToUnixTimestampMs(this.EndTime);

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
            return HashCode.Combine(this.Interval);
        }

        public override string ToString()
        {
            return "Window{" +
                $"startMs={this.Start()}" +
                $", endMs={this.End()}" +
                '}';
        }
    }
}
