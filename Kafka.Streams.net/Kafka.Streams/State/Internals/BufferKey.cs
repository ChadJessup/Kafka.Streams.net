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
    public class BufferKey : Comparable<BufferKey>
    {
        private long time;
        private Bytes key;

        BufferKey(long time, Bytes key)
        {
            this.time = time;
            this.key = key;
        }

        long time()
        {
            return time;
        }

        Bytes key()
        {
            return key;
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || GetType() != o.GetType())
            {
                return false;
            }
            BufferKey bufferKey = (BufferKey)o;
            return time == bufferKey.time &&
                Objects.Equals(key, bufferKey.key);
        }

        public override int GetHashCode()
        {
            return Objects.hash(time, key);
        }

        public override int compareTo(BufferKey o)
        {
            // ordering of keys within a time uses GetHashCode().
            int timeComparison = long.compare(time, o.time);
            return timeComparison == 0 ? key.compareTo(o.key) : timeComparison;
        }

        public override string ToString()
        {
            return "BufferKey{" +
                "key=" + key +
                ", time=" + time +
                '}';
        }
    }
}