using System;
using Kafka.Common.Extensions;

namespace Kafka.Streams.KStream
{
    public abstract class Window
    {
        protected long startMs;
        protected long endMs;
        private readonly DateTime startTime;
        private readonly DateTime endTime;


        /**
         * Create a new window for the given start and end time.
         *
         * @param startMs the start timestamp of the window
         * @param endMs   the end timestamp of the window
         * @throws ArgumentException if {@code startMs} is negative or if {@code endMs} is smaller than {@code startMs}
         */
        public Window(long startMs, long endMs)
        {
            if (startMs < 0)
            {
                throw new System.ArgumentException("Window startMs time cannot be negative.");
            }
            if (endMs < startMs)
            {
                throw new System.ArgumentException("Window endMs time cannot be smaller than window startMs time.");
            }
            this.startMs = startMs;
            this.endMs = endMs;

            //this.startTime = DateTime.ofEpochMilli(startMs);
            //this.endTime = DateTime.ofEpochMilli(endMs);
        }

        /**
         * Return the start timestamp of this window.
         *
         * @return The start timestamp of this window.
         */
        public long start()
        {
            return startMs;
        }

        /**
         * Return the end timestamp of this window.
         *
         * @return The end timestamp of this window.
         */
        public long end()
        {
            return endMs;
        }

        /**
         * Check if the given window overlaps with this window.
         * Should throw an {@link ArgumentException} if the {@code other} window has a different type than {@code
         * this} window.
         *
         * @param other another window of the same type
         * @return {@code true} if {@code other} overlaps with this window&mdash;{@code false} otherwise
         */
        public abstract bool overlap(Window other);

        public bool equals(Object obj)
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

        public int hashCode()
        {
            return (int)(((startMs << 32) | endMs) % 0xFFFFFFFFL);
        }

        public String toString()
        {
            return "Window{" +
                "startMs=" + startMs +
                ", endMs=" + endMs +
                '}';
        }

    }
}
