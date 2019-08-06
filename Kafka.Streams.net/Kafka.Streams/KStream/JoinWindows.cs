/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
     * For time semantics, see {@link TimestampExtractor}.
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
     * @see TimestampExtractor
     */
    public class JoinWindows : Windows<Window>
    {

        private long maintainDurationMs;

        /** Maximum time difference for tuples that are before the join tuple. */
        public long beforeMs;
        /** Maximum time difference for tuples that are after the join tuple. */
        public long afterMs;

        private long graceMs;

        private JoinWindows(long beforeMs,
                             long afterMs,
                             long graceMs,
                             long maintainDurationMs)
        {
            if (beforeMs + afterMs < 0)
            {
                throw new System.ArgumentException("Window interval (ie, beforeMs+afterMs) must not be negative.");
            }
            this.afterMs = afterMs;
            this.beforeMs = beforeMs;
            this.graceMs = graceMs;
            this.maintainDurationMs = maintainDurationMs;
        }

        [System.Obsolete] // removing segments from Windows will fix this
        private JoinWindows(long beforeMs,
                             long afterMs,
                             long graceMs,
                             long maintainDurationMs,
                             int segments)
            : base(segments)
        {
            if (beforeMs + afterMs < 0)
            {
                throw new System.ArgumentException("Window interval (ie, beforeMs+afterMs) must not be negative.");
            }
            this.afterMs = afterMs;
            this.beforeMs = beforeMs;
            this.graceMs = graceMs;
            this.maintainDurationMs = maintainDurationMs;
        }

        /**
         * Specifies that records of the same key are joinable if their timestamps are within {@code timeDifferenceMs},
         * i.e., the timestamp of a record from the secondary stream is max {@code timeDifferenceMs} earlier or later than
         * the timestamp of the record from the primary stream.
         *
         * @param timeDifferenceMs join window interval in milliseconds
         * @throws ArgumentException if {@code timeDifferenceMs} is negative
         * @deprecated Use {@link #of(Duration)} instead.
         */
        [System.Obsolete]
        public static JoinWindows of(long timeDifferenceMs)
        {
            // This is a static factory method, so we initialize grace and retention to the defaults.
            return new JoinWindows(timeDifferenceMs, timeDifferenceMs, -1L, DEFAULT_RETENTION_MS);
        }

        /**
         * Specifies that records of the same key are joinable if their timestamps are within {@code timeDifference},
         * i.e., the timestamp of a record from the secondary stream is max {@code timeDifference} earlier or later than
         * the timestamp of the record from the primary stream.
         *
         * @param timeDifference join window interval
         * @throws ArgumentException if {@code timeDifference} is negative or can't be represented as {@code long milliseconds}
         */
        public static JoinWindows of(TimeSpan timeDifference)
        {
            string msgPrefix = prepareMillisCheckFailMsgPrefix(timeDifference, "timeDifference");
            return of(ApiUtils.validateMillisecondDuration(timeDifference, msgPrefix));
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
         * @deprecated Use {@link #before(Duration)} instead.
         */
        [System.Obsolete]
        public JoinWindows before(long timeDifferenceMs)
        {
            return new JoinWindows(timeDifferenceMs, afterMs, graceMs, maintainDurationMs, segments);
        }

        /**
         * Changes the start window boundary to {@code timeDifference} but keep the end window boundary as is.
         * Thus, records of the same key are joinable if the timestamp of a record from the secondary stream is at most
         * {@code timeDifference} earlier than the timestamp of the record from the primary stream.
         * {@code timeDifference} can be negative but its absolute value must not be larger than current window "after"
         * value (which would result in a negative window size).
         *
         * @param timeDifference relative window start time
         * @throws ArgumentException if the resulting window size is negative or {@code timeDifference} can't be represented as {@code long milliseconds}
         */
        public JoinWindows before(TimeSpan timeDifference)
        {
            string msgPrefix = prepareMillisCheckFailMsgPrefix(timeDifference, "timeDifference");
            return before(ApiUtils.validateMillisecondDuration(timeDifference, msgPrefix));
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
         * @deprecated Use {@link #after(Duration)} instead
         */
        [System.Obsolete]
        public JoinWindows after(long timeDifferenceMs)
        {
            return new JoinWindows(beforeMs, timeDifferenceMs, graceMs, maintainDurationMs, segments);
        }

        /**
         * Changes the end window boundary to {@code timeDifference} but keep the start window boundary as is.
         * Thus, records of the same key are joinable if the timestamp of a record from the secondary stream is at most
         * {@code timeDifference} later than the timestamp of the record from the primary stream.
         * {@code timeDifference} can be negative but its absolute value must not be larger than current window "before"
         * value (which would result in a negative window size).
         *
         * @param timeDifference relative window end time
         * @throws ArgumentException if the resulting window size is negative or {@code timeDifference} can't be represented as {@code long milliseconds}
         */
        public JoinWindows after(TimeSpan timeDifference)
        {
            string msgPrefix = prepareMillisCheckFailMsgPrefix(timeDifference, "timeDifference");
            return after(ApiUtils.validateMillisecondDuration(timeDifference, msgPrefix));
        }

        /**
         * Not supported by {@code JoinWindows}.
         * Throws {@link InvalidOperationException}.
         *
         * @throws InvalidOperationException at every invocation
         */

        public Dictionary<long, Window> windowsFor(long timestamp)
        {
            throw new InvalidOperationException("windowsFor() is not supported by JoinWindows.");
        }


        public long size()
        {
            return beforeMs + afterMs;
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

        public JoinWindows grace(TimeSpan afterWindowEnd)
        {
            string msgPrefix = prepareMillisCheckFailMsgPrefix(afterWindowEnd, "afterWindowEnd");
            long afterWindowEndMs = ApiUtils.validateMillisecondDuration(afterWindowEnd, msgPrefix);
            if (afterWindowEndMs < 0)
            {
                throw new System.ArgumentException("Grace period must not be negative.");
            }
            return new JoinWindows(beforeMs, afterMs, afterWindowEndMs, maintainDurationMs, segments);
        }


        public long gracePeriodMs()
        {
            // NOTE: in the future, when we Remove maintainMs,
            // we should default the grace period to 24h to maintain the default behavior,
            // or we can default to (24h - size) if you want to be base.accurate.
            return graceMs != -1 ? graceMs : maintainMs() - size();
        }

        /**
         * @param durationMs the window retention time in milliseconds
         * @return itself
         * @throws ArgumentException if {@code durationMs} is smaller than the window size
         * @deprecated since 2.1. Use {@link JoinWindows#grace(Duration)} instead.
         */

        [System.Obsolete]
        public JoinWindows until(long durationMs)
        {
            if (durationMs < size())
            {
                throw new System.ArgumentException("Window retention time (durationMs) cannot be smaller than the window size.");
            }
            return new JoinWindows(beforeMs, afterMs, graceMs, durationMs, segments);
        }

        /**
         * {@inheritDoc}
         * <p>
         * For {@link TimeWindows} the maintain duration is at least as small as the window size.
         *
         * @return the window maintain duration
         * @deprecated since 2.1. This function should not be used anymore, since {@link JoinWindows#until(long)}
         *             is deprecated in favor of {@link JoinWindows#grace(Duration)}.
         */

        [System.Obsolete]
        public long maintainMs()
        {
            return Math.Max(maintainDurationMs, size());
        }



        public bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || GetType() != o.GetType())
            {
                return false;
            }
            JoinWindows that = (JoinWindows)o;
            return beforeMs == that.beforeMs &&
                afterMs == that.afterMs &&
                maintainDurationMs == that.maintainDurationMs &&
                segments == that.segments &&
                graceMs == that.graceMs;
        }



        public int hashCode()
        {
            return Objects.hash(beforeMs, afterMs, graceMs, maintainDurationMs, segments);
        }



        public string ToString()
        {
            return "JoinWindows{" +
                "beforeMs=" + beforeMs +
                ", afterMs=" + afterMs +
                ", graceMs=" + graceMs +
                ", maintainDurationMs=" + maintainDurationMs +
                ", segments=" + segments +
                '}';
        }
    }
}