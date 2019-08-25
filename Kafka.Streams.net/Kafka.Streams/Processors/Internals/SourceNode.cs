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
using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processor.Interfaces;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Processor.Internals
{
    public class SourceNode<K, V> : ProcessorNode<K, V>
    {
        private List<string> topics;

        private IProcessorContext<K, V> context;
        private IDeserializer<K> keyDeserializer;
        private IDeserializer<V> valDeserializer;
        private ITimestampExtractor timestampExtractor;

        public SourceNode(
            string name,
            List<string> topics,
            ITimestampExtractor timestampExtractor,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valDeserializer)
            : base(name)
        {
            this.topics = topics;
            this.timestampExtractor = timestampExtractor;
            this.keyDeserializer = keyDeserializer;
            this.valDeserializer = valDeserializer;
        }

        public SourceNode(
            string name,
            List<string> topics,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valDeserializer)
            : this(name, topics, null, keyDeserializer, valDeserializer)
        {
        }

        public K deserializeKey(string topic, Headers headers, byte[] data)
        {
            return keyDeserializer.Deserialize(data, false, new SerializationContext(MessageComponentType.Key, topic));
        }

        public V deserializeValue(string topic, Headers headers, byte[] data)
        {
            return valDeserializer.Deserialize(data, data == null, new SerializationContext(MessageComponentType.Value, topic));
        }

        public override void init(IInternalProcessorContext<K, V> context)
        {
            base.init(context);
            this.context = context;

            // if deserializers are null, get the default ones from the context
            if (this.keyDeserializer == null)
            {
                this.keyDeserializer = (IDeserializer<K>)context.keySerde.Deserializer;
            }
            if (this.valDeserializer == null)
            {
                this.valDeserializer = (IDeserializer<V>)context.valueSerde.Deserializer;
            }

            // if value deserializers are for {@code Change} values, set the inner deserializer when necessary
            //if (this.valDeserializer is ChangedDeserializer<V> &&
            //        ((ChangedDeserializer<V>)this.valDeserializer).inner() == null)
            //{
            //    ((ChangedDeserializer<V>)this.valDeserializer).setInner(context.valueSerde.Deserializer);
            //}
        }

        public override void process(K key, V value)
        {
            context.forward(key, value);
            //sourceNodeForwardSensor.record();
        }

        /**
         * @return a string representation of this node, useful for debugging.
         */

        public override string ToString()
        {
            return ToString("");
        }

        /**
         * @return a string representation of this node starting with the given indent, useful for debugging.
         */
        public override string ToString(string indent)
        {
            StringBuilder sb = new StringBuilder(base.ToString(indent));
            sb.Append(indent).Append("\ttopics:\t\t[");
            foreach (string topic in topics)
            {
                sb.Append(topic);
                sb.Append(", ");
            }
            sb.Length = sb.Length - 2;  // Remove the last comma
            sb.Append("]\n");
            return sb.ToString();
        }

        public ITimestampExtractor getTimestampExtractor()
        {
            return timestampExtractor;
        }
    }
}