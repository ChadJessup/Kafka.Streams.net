using Kafka.Streams.Internals;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream
{
    /**
     * The window specifications used for joins.
     * <p>
     * A {@code JoinWindows} instance defines a maximum time difference for a {@link KStream#join(KStream, ValueJoiner,
     * JoinWindows) join over two streams} on the same key.
     * In SQL-style you would express this join as
     * <pre>{@code
     *     SELECT * FROM stream1, stream2
     *     WHERE
     *       stream1.key = stream2.key
     *       AND
     *       stream1.ts - before <= stream2.ts AND stream2.ts <= stream1.ts + after
     * }</pre>
     * There are three different window configuration supported:
     * <ul>
     *     <li>before = after = time-difference</li>
     *     <li>before = 0 and after = time-difference</li>
     *     <li>before = time-difference and after = 0</li>
     * </ul>
     * A join is symmetric in the sense, that a join specification on the first stream returns the same result record as
     * a join specification on the second stream with flipped before and after values.
     * <p>
     * Both values (before and after) must not result in an "inverse" window, i.e., upper-interval bound cannot be smaller
     * than lower-interval bound.
     * <p>
     * {@code JoinWindows} are sliding windows, thus, they are aligned to the actual record timestamps.
     * This implies, that each input record defines its own window with start and end time being relative to the record's
     * timestamp.
     * <p>
     * For time semantics, see {@link ITimestampExtractor}.
     *
     * @see TimeWindows
     * @see UnlimitedWindows
     * @see SessionWindows
     * @see KStream#join(KStream, ValueJoiner, JoinWindows)
     * @see KStream#join(KStream, ValueJoiner, JoinWindows, Joined)
     * @see KStream#leftJoin(KStream, ValueJoiner, JoinWindows)
     * @see KStream#leftJoin(KStream, ValueJoiner, JoinWindows, Joined)
     * @see KStream#outerJoin(KStream, ValueJoiner, JoinWindows)
     * @see KStream#outerJoin(KStream, ValueJoiner, JoinWindows, Joined)
     * @see ITimestampExtractor
     */
    public class JoinWindows : Windows<Window>
    {
        private readonly TimeSpan maintainDuration;

        /** Maximum time difference for tuples that are before the join tuple. */
        public TimeSpan before { get; }
        /** Maximum time difference for tuples that are after the join tuple. */
        public TimeSpan after { get; }

        private readonly TimeSpan grace;

        private JoinWindows(
            TimeSpan before,
            TimeSpan after,
            TimeSpan grace,
            TimeSpan maintainDuration)
        {
            if ((before + after) < TimeSpan.Zero)
            {
                throw new ArgumentException("Window interval (ie, beforeMs+afterMs) must not be negative.");
            }

            this.after = after;
            this.before = before;
            this.grace = grace;
            this.maintainDuration = maintainDuration;
        }

        [Obsolete] // removing segments from Windows will fix this
        private JoinWindows(
            TimeSpan before,
            TimeSpan after,
            TimeSpan grace,
            TimeSpan maintainDuration,
            int segments)
            : base(segments)
        {
            if ((before + after) < TimeSpan.Zero)
            {
                throw new ArgumentException("Window interval (ie, beforeMs+afterMs) must not be negative.");
            }

            this.after = after;
            this.before = before;
            this.grace = grace;
            this.maintainDuration = maintainDuration;
        }

        /**
         * Specifies that records of the same key are joinable if their timestamps are within {@code timeDifferenceMs},
         * i.e., the timestamp of a record from the secondary stream is max {@code timeDifferenceMs} earlier or later than
         * the timestamp of the record from the primary stream.
         *
         * @param timeDifferenceMs join window interval in milliseconds
         * @throws ArgumentException if {@code timeDifferenceMs} is negative
         */
        public static JoinWindows Of(TimeSpan timeDifference)
        {
            // This is a static factory method, so we initialize grace and retention to the defaults.
            return new JoinWindows(
                timeDifference,
                timeDifference,
                TimeSpan.FromMilliseconds(-1L),
                WindowingDefaults.DefaultRetention);
        }

        /**
         * Changes the start window boundary to {@code timeDifferenceMs} but keep the end window boundary as is.
         * Thus, records of the same key are joinable if the timestamp of a record from the secondary stream is at most
         * {@code timeDifferenceMs} earlier than the timestamp of the record from the primary stream.
         * {@code timeDifferenceMs} can be negative but its absolute value must not be larger than current window "after"
         * value (which would result in a negative window size).
         *
         * @param timeDifferenceMs relative window start time in milliseconds
         * @throws ArgumentException if the resulting window size is negative
         * @deprecated Use {@link #before(TimeSpan)} instead.
         */
        [Obsolete]
        public JoinWindows Before(TimeSpan timeDifference)
        {
            return new JoinWindows(timeDifference, this.after, this.grace, this.maintainDuration, this.segments);
        }

        /**
         * Changes the end window boundary to {@code timeDifferenceMs} but keep the start window boundary as is.
         * Thus, records of the same key are joinable if the timestamp of a record from the secondary stream is at most
         * {@code timeDifferenceMs} later than the timestamp of the record from the primary stream.
         * {@code timeDifferenceMs} can be negative but its absolute value must not be larger than current window "before"
         * value (which would result in a negative window size).
         *
         * @param timeDifferenceMs relative window end time in milliseconds
         * @throws ArgumentException if the resulting window size is negative
         * @deprecated Use {@link #after(TimeSpan)} instead
         */
        [Obsolete]
        public JoinWindows After(TimeSpan timeDifference)
        {
            return new JoinWindows(
                this.before,
                timeDifference,
                this.grace,
                this.maintainDuration,
                this.segments);
        }

        /**
         * Not supported by {@code JoinWindows}.
         * Throws {@link InvalidOperationException}.
         *
         * @throws InvalidOperationException at every invocation
         */
        public override Dictionary<DateTime, Window> WindowsFor(DateTime timestamp)
        {
            throw new InvalidOperationException("WindowsFor() is not supported by JoinWindows.");
        }

        public override TimeSpan Size()
        {
            return this.before + this.after;
        }

        /**
         * Reject late events that arrive more than {@code afterWindowEnd}
         * after the end of its window.
         *
         * Lateness is defined as (stream_time - record_timestamp).
         *
         * @param afterWindowEnd The grace period to admit late-arriving events to a window.
         * @return this updated builder
         * @throws ArgumentException if the {@code afterWindowEnd} is negative of can't be represented as {@code long milliseconds}
         */
        public JoinWindows Grace(TimeSpan afterWindowEnd)
        {
            var msgPrefix = ApiUtils.PrepareMillisCheckFailMsgPrefix(afterWindowEnd, "afterWindowEnd");
            var validatedAfterWindowEnd = ApiUtils.ValidateMillisecondDuration(afterWindowEnd, msgPrefix);

            if (validatedAfterWindowEnd < TimeSpan.Zero)
            {
                throw new ArgumentException("Grace period must not be negative.");
            }

            return new JoinWindows(
                this.before,
                this.after,
                validatedAfterWindowEnd,
                this.maintainDuration,
                this.segments);
        }


        public override TimeSpan GracePeriod()
        {
            // NOTE: in the future, when we Remove maintainMs,
            // we should default the grace period to 24h to maintain the default behavior,
            // or we can default to (24h - size) if you want to be base.accurate.
            return this.grace != TimeSpan.FromMilliseconds(-1)
                ? this.grace
                : this.MaintainDuration() - this.Size();
        }

        /**
         * @param durationMs the window retention time in milliseconds
         * @return itself
         * @throws ArgumentException if {@code durationMs} is smaller than the window size
         * @deprecated since 2.1. Use {@link JoinWindows#grace(TimeSpan)} instead.
         */
        [Obsolete]
        public new JoinWindows Until(TimeSpan duration)
        {
            if (duration < this.Size())
            {
                throw new ArgumentException("Window retention time (durationMs) cannot be smaller than the window size.");
            }

            return new JoinWindows(
                this.before,
                this.after,
                this.grace,
                duration,
                this.segments);
        }

        /**
         * {@inheritDoc}
         * <p>
         * For {@link TimeWindows} the maintain duration is at least as small as the window size.
         *
         * @return the window maintain duration
         * @deprecated since 2.1. This function should not be used anymore, since {@link JoinWindows#until(long)}
         *             is deprecated in favor of {@link JoinWindows#grace(TimeSpan)}.
         */

        [Obsolete]
        public override TimeSpan MaintainDuration()
        {
            return TimeSpan.FromMilliseconds(Math.Max(this.maintainDuration.TotalMilliseconds, this.Size().TotalMilliseconds));
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || this.GetType() != o.GetType())
            {
                return false;
            }

            var that = (JoinWindows)o;

            return this.before == that.before &&
                this.after == that.after &&
                this.maintainDuration == that.maintainDuration &&
                this.segments == that.segments &&
                this.grace == that.grace;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                this.before,
                this.after,
                this.grace,
                this.maintainDuration,
                this.segments);
        }

        public override string ToString()
        {
            return "JoinWindows{" +
                   $"beforeMs={this.before}" +
                   $", afterMs={this.after}" +
                   $", graceMs={this.grace}" +
                   $", maintainDurationMs={this.maintainDuration}" +
                   $", segments={this.segments}" +
                   "}";
        }
    }
}
