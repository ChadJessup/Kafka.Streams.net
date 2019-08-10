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
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.IProcessor.Internals;

namespace Kafka.Streams.State.Internals
{
    public class Eviction<K, V>
    {
        public K key { get; }
        public Change<V> value { get; }
        private ProcessorRecordContext recordContext;

        public Eviction(K key, Change<V> value, ProcessorRecordContext recordContext)
        {
            this.key = key;
            this.value = value;
            this.recordContext = recordContext;
        }

        public override string ToString()
        {
            return "Eviction{key=" + key + ", value=" + value + ", recordContext=" + recordContext + '}';
        }


        public override bool Equals(object o)
        {
            if (this == o) return true;
            if (o == null || GetType() != o.GetType()) return false;

            var eviction = (Eviction<object, object>)o;

            return key.Equals(eviction.key) &&
                value.Equals(eviction.value) &&
                recordContext.Equals(eviction.recordContext);
        }

        public override int GetHashCode()
        {
            return (key, value, recordContext).GetHashCode();
        }
    }
}