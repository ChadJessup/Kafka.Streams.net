using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream
{
    /**
     * The window specification for fixed size windows that is used to define window boundaries and grace period.
     *
     * Grace period defines how long to wait on late events, where lateness is defined as (stream_time - record_timestamp).
     *
     * Warning: It may be unsafe to use objects of this class in set- or map-like collections,
     * since the Equals and hashCode methods depend on mutable fields.
     *
     * @param <W> type of the window instance
     * @see TimeWindows
     * @see UnlimitedWindows
     * @see JoinWindows
     * @see SessionWindows
     * @see TimestampExtractor
     */
    public abstract class Windows<W>
        where W : Window
    {
        private TimeSpan maintainRetentionDuration = WindowingDefaults.DefaultRetention;

        [Obsolete]
        public int segments { get; protected set; } = 3;

        protected Windows()
        { }

        [Obsolete]// remove this constructor when we remove segments.
        public Windows(int segments)
        {
            this.segments = segments;
        }

        /**
         * Set the window maintain duration (retention time) in milliseconds.
         * This retention time is a guaranteed <i>lower bound</i> for how long a window will be maintained.
         *
         * @param durationMs the window retention time in milliseconds
         * @return itself
         * @throws ArgumentException if {@code durationMs} is negative
         * @deprecated since 2.1. Use {@link Materialized#withRetention(TimeSpan)}
         *             or directly configure the retention in a store supplier and use {@link Materialized#as(WindowBytesStoreSupplier)}.
         */
        [Obsolete]
        public Windows<W> Until(TimeSpan duration)
        {
            if (duration < TimeSpan.Zero)
            {
                throw new ArgumentException("Window retention time (durationMs) cannot be negative.");
            }

            this.maintainRetentionDuration = duration;

            return this;
        }

        /**
         * Return the window maintain duration (retention time) in milliseconds.
         *
         * @return the window maintain duration
         * @deprecated since 2.1. Use {@link Materialized#retention} instead.
         */
        [Obsolete]
        public virtual TimeSpan MaintainDuration()
        {
            return this.maintainRetentionDuration;
        }

        /**
         * Set the number of segments to be used for rolling the window store.
         * This function is not exposed to users but can be called by developers that extend this class.
         *
         * @param segments the number of segments to be used
         * @return itself
         * @throws ArgumentException if specified segments is small than 2
         * @deprecated since 2.1 Override segmentInterval() instead.
         */
        [Obsolete]
        public Windows<W> Segments(int segments)
        {
            if (segments < 2)
            {
                throw new ArgumentException("Number of segments must be at least 2.");
            }

            this.segments = segments;

            return this;
        }

        /**
         * Create All windows that contain the provided timestamp, indexed by non-negative window start timestamps.
         *
         * @param timestamp the timestamp window should get created for
         * @return a map of {@code windowStartTimestamp -> Window} entries
         */
        public abstract Dictionary<DateTime, W>? WindowsFor(DateTime timestamp);

        /**
         * Return the size of the specified windows in milliseconds.
         *
         * @return the size of the specified windows
         */
        public abstract TimeSpan Size();
    }
}
