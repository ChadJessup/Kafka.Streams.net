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
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{

    public interface Segments<S>
        where S : ISegment
    {
        long segmentId(long timestamp);

        string segmentName(long segmentId);

        S getSegmentForTimestamp(long timestamp);

        S getOrCreateSegmentIfLive<K, V>(long segmentId, IInternalProcessorContext context, long streamTime);

        S getOrCreateSegment<K, V>(long segmentId, IInternalProcessorContext context);

        void openExisting<K, V>(IInternalProcessorContext context, long streamTime);

        List<S> segments(long timeFrom, long timeTo);

        List<S> allSegments();

        void flush();

        void close();
    }
}