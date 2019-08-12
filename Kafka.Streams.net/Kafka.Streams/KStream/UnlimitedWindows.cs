///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with
// * this work for.Additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//using Kafka.Streams.Internals.Kafka.Streams.Internals;
//using Kafka.Streams.KStream.Internals;
//using System;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream
//{













//    /**
//     * The unlimited window specifications used for aggregations.
//     * <p>
//     * An unlimited time window is also called landmark window.
//     * It has a fixed starting point while its window end is defined as infinite.
//     * With this regard, it is a fixed-size window with infinite window size.
//     * <p>
//     * For time semantics, see {@link ITimestampExtractor}.
//     *
//     * @see TimeWindows
//     * @see SessionWindows
//     * @see JoinWindows
//     * @see KGroupedStream#windowedBy(Windows)
//     * @see ITimestampExtractor
//     */
//    public class UnlimitedWindows : Windows<UnlimitedWindow>
//    {

//        private static long DEFAULT_START_TIMESTAMP_MS = 0L;

//        /** The start timestamp of the window. */

//        public long startMs;

//        private UnlimitedWindows(long startMs)
//        {
//            this.startMs = startMs;
//        }

//        /**
//         * Return an unlimited window starting at timestamp zero.
//         */
//        public static UnlimitedWindows of()
//        {
//            return new UnlimitedWindows(DEFAULT_START_TIMESTAMP_MS);
//        }

//        /**
//         * Return a new unlimited window for the specified start timestamp.
//         *
//         * @param startMs the window start time
//         * @return a new unlimited window that starts at {@code startMs}
//         * @throws ArgumentException if the start time is negative
//         * @deprecated Use {@link #startOn(Instant)} instead
//         */
//        [System.Obsolete]
//        public UnlimitedWindows startOn(long startMs)
//        {
//            if (startMs < 0)
//            {
//                throw new System.ArgumentException("Window start time (startMs) cannot be negative.");
//            }
//            return new UnlimitedWindows(startMs);
//        }

//        /**
//         * Return a new unlimited window for the specified start timestamp.
//         *
//         * @param start the window start time
//         * @return a new unlimited window that starts at {@code start}
//         * @throws ArgumentException if the start time is negative or can't be represented as {@code long milliseconds}
//         */
//        public UnlimitedWindows startOn(DateTime start)
//        {
//            string msgPrefix = prepareMillisCheckFailMsgPrefix(start, "start");
//            return startOn(ApiUtils.validateMillisecondInstant(start, msgPrefix));
//        }


//        public Dictionary<long, UnlimitedWindow> windowsFor(long timestamp)
//        {
//            // always return the single unlimited window

//            // we cannot use Collections.singleMap since it does not support Remove()
//            Dictionary<long, UnlimitedWindow> windows = new Dictionary<long, UnlimitedWindow>();
//            if (timestamp >= startMs)
//            {
//                windows.Add(startMs, new UnlimitedWindow(startMs));
//            }
//            return windows;
//        }

//        /**
//         * {@inheritDoc}
//         * As unlimited windows have conceptually infinite size, this methods just returns {@link long#MaxValue}.
//         *
//         * @return the size of the specified windows which is {@link long#MaxValue}
//         */

//        public long size()
//        {
//            return long.MaxValue;
//        }

//        /**
//         * Throws an {@link ArgumentException} because the retention time for unlimited windows is always infinite
//         * and cannot be changed.
//         *
//         * @throws ArgumentException on every invocation.
//         * @deprecated since 2.1.
//         */

//        [System.Obsolete]
//        public UnlimitedWindows until(long durationMs)
//        {
//            throw new System.ArgumentException("Window retention time (durationMs) cannot be set for UnlimitedWindows.");
//        }

//        /**
//         * {@inheritDoc}
//         * The retention time for unlimited windows in infinite and thus represented as {@link long#MaxValue}.
//         *
//         * @return the window retention time that is {@link long#MaxValue}
//         * @deprecated since 2.1. Use {@link Materialized#retention} instead.
//         */

//        [System.Obsolete]
//        public long maintainMs()
//        {
//            return long.MaxValue;
//        }


//        public long gracePeriodMs()
//        {
//            return 0L;
//        }



//        public bool Equals(object o)
//        {
//            if (this == o)
//            {
//                return true;
//            }
//            if (o == null || GetType() != o.GetType())
//            {
//                return false;
//            }
//            UnlimitedWindows that = (UnlimitedWindows)o;
//            return startMs == that.startMs && segments == that.segments;
//        }



//        public int hashCode()
//        {
//            return Objects.hash(startMs, segments);
//        }



//        public string ToString()
//        {
//            return "UnlimitedWindows{" +
//                "startMs=" + startMs +
//                ", segments=" + segments +
//                '}';
//        }
//    }
//}
