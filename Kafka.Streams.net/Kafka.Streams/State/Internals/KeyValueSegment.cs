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
namespace Kafka.Streams.State.Internals
{


    using Kafka.Common.Utils.Utils;
    using Kafka.Streams.Processor.IProcessorContext;




    class KeyValueSegment : RocksDbStore : Comparable<KeyValueSegment>, Segment
    {
        public long id;

        KeyValueSegment(string segmentName,
                        string windowName,
                        long id)
        {
            base(segmentName, windowName);
            this.id = id;
        }

        public override void destroy()
        {
            Utils.delete(dbDir);
        }

        public override int CompareTo(KeyValueSegment segment)
        {
            return long.compare(id, segment.id);
        }

        public override void openDB(IProcessorContext<K, V> context)
        {
            base.openDB(context);
            // skip the registering step
            internalProcessorContext = context;
        }

        public override string ToString()
        {
            return "KeyValueSegment(id=" + id + ", name=" + name() + ")";
        }

        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }
            KeyValueSegment segment = (KeyValueSegment)obj;
            return id == segment.id;
        }

        public override int GetHashCode()
        {
            return Objects.hash(id);
        }
    }
}