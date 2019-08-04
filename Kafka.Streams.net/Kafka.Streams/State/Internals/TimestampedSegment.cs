/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
namespace Kafka.streams.state.internals;

using Kafka.Common.Utils.Utils;
using Kafka.Streams.Processor.IProcessorContext;

import java.io.IOException;
import java.util.Objects;

class TimestampedSegment : RocksDBTimestampedStore : Comparable<TimestampedSegment>, Segment
{
    public long id;

    TimestampedSegment(string segmentName,
                       string windowName,
                       long id)
{
        super(segmentName, windowName);
        this.id = id;
    }

    public override void destroy() throws IOException
{
        Utils.delete(dbDir);
    }

    public override int compareTo(TimestampedSegment segment)
{
        return Long.compare(id, segment.id);
    }

    public override void openDB(IProcessorContext context)
{
        super.openDB(context);
        // skip the registering step
        internalProcessorContext = context;
    }

    public override string ToString()
{
        return "TimestampedSegment(id=" + id + ", name=" + name() + ")";
    }

    public override bool Equals(object obj)
{
        if (obj == null || GetType() != obj.GetType())
{
            return false;
        }
        TimestampedSegment segment = (TimestampedSegment) obj;
        return id == segment.id;
    }

    public override int GetHashCode()
{
        return Objects.hash(id);
    }
}
