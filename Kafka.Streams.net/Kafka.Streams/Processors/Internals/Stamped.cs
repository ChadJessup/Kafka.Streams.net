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
using System;

namespace Kafka.Streams.Processor.Internals
{


    public class Stamped<V> : IComparable
    {
        public V value;
        public long timestamp;

        public Stamped(V value, long timestamp)
        {
            this.value = value;
            this.timestamp = timestamp;
        }


        public int CompareTo(object other)
        {
            long otherTimestamp = ((Stamped<object>)other).timestamp;

            if (timestamp < otherTimestamp)
            {
                return -1;
            }
            else if (timestamp > otherTimestamp)
            {
                return 1;
            }
            return 0;
        }


        public override bool Equals(object other)
        {
            if (other == null || this.GetType() != other.GetType())
            {
                return false;
            }

            long otherTimestamp = ((Stamped<object>)other).timestamp;

            return timestamp == otherTimestamp;
        }


        public override int GetHashCode()
        {
            return (timestamp).GetHashCode();
        }
    }
}