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
//namespace Kafka.Streams.State.Internals
//{
//    /**
//     * Manages the {@link TimestampedSegment}s that are used by the {@link RocksDbTimestampedSegmentedBytesStore}
//     */
//    public class TimestampedSegments : AbstractSegments<TimestampedSegment>
//    {

//        TimestampedSegments(string name,
//                            long retentionPeriod,
//                            long segmentInterval)
//            : base(name, retentionPeriod, segmentInterval)
//        {
//        }

//        public override TimestampedSegment getOrCreateSegment(long segmentId,
//                                                     IInternalProcessorContext<K, V> context)
//        {
//            if (segments.ContainsKey(segmentId))
//            {
//                return segments[segmentId];
//            }
//            else
//            {
//                TimestampedSegment newSegment = new TimestampedSegment(segmentName(segmentId), name, segmentId);

//                if (segments.Add(segmentId, newSegment) != null)
//                {
//                    throw new InvalidOperationException("TimestampedSegment already exists. Possible concurrent access.");
//                }

//                newSegment.openDB(context);
//                return newSegment;
//            }
//        }
//    }
//}