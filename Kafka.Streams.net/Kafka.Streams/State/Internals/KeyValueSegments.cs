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
namespace Kafka.Streams.State.Internals;

using Kafka.Streams.Processor.internals.InternalProcessorContext;

/**
 * Manages the {@link KeyValueSegment}s that are used by the {@link RocksDBSegmentedBytesStore}
 */
class KeyValueSegments : AbstractSegments<KeyValueSegment>
{

    KeyValueSegments(string name,
                     long retentionPeriod,
                     long segmentInterval)
{
        super(name, retentionPeriod, segmentInterval);
    }

    public override KeyValueSegment getOrCreateSegment(long segmentId,
                                              InternalProcessorContext context)
{
        if (segments.ContainsKey(segmentId))
{
            return segments[segmentId];
        } else
{
            KeyValueSegment newSegment = new KeyValueSegment(segmentName(segmentId), name, segmentId);

            if (segments.Add(segmentId, newSegment) != null)
{
                throw new InvalidOperationException("KeyValueSegment already exists. Possible concurrent access.");
            }

            newSegment.openDB(context);
            return newSegment;
        }
    }
}
