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
//namespace Kafka.Streams.KStream.Internals
//{




//    /**
//     * A session window covers a closed time interval with its start and end timestamp both being an inclusive boundary.
//     * <p>
//     * For time semantics, see {@link org.apache.kafka.streams.processor.ITimestampExtractor ITimestampExtractor}.
//     *
//     * @see TimeWindow
//     * @see UnlimitedWindow
//     * @see org.apache.kafka.streams.kstream.SessionWindows
//     * @see org.apache.kafka.streams.processor.ITimestampExtractor
//     */
//    public class SessionWindow : Window
//    {


//        /**
//         * Create a new window for the given start time and end time (both inclusive).
//         *
//         * @param startMs the start timestamp of the window
//         * @param endMs   the end timestamp of the window
//         * @throws ArgumentException if {@code startMs} is negative or if {@code endMs} is smaller than {@code startMs}
//         */
//        public SessionWindow(long startMs, long endMs)
//            : base(startMs, endMs)
//        {
//        }

//        /**
//         * Check if the given window overlaps with this window.
//         *
//         * @param other another window
//         * @return {@code true} if {@code other} overlaps with this window&mdash;{@code false} otherwise
//         * @throws ArgumentException if the {@code other} window has a different type than {@code this} window
//         */
//        public bool overlap(Window other)
//        {
//            if (GetType() != other.GetType())
//            {
//                throw new System.ArgumentException("Cannot compare windows of different type. Other window has type "
//                    + other.GetType() + ".");
//            }
//            SessionWindow otherWindow = (SessionWindow)other;
//            return !(otherWindow.endMs < startMs || endMs < otherWindow.startMs);
//        }
//    }
//}
