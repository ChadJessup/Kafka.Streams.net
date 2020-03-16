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
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamTransformValuesProcessor<K, V, R> : IKeyValueProcessor<K, V>
    {
        private readonly IValueTransformerWithKey<K, V, R> valueTransformer;
        private IProcessorContext context;

        public KStreamTransformValuesProcessor(IValueTransformerWithKey<K, V, R> valueTransformer)
        {
            this.valueTransformer = valueTransformer;
        }


        public void Init(IProcessorContext context)
        {
            valueTransformer.init(new ForwardingDisabledProcessorContext<K, V>(context));
            this.context = context;
        }


        public void Process(K key, V value)
        {
            //context.forward(key, valueTransformer.transform(key, value));
        }

        public void Close()
        {
            valueTransformer.close();
        }

        public void Process<K1, V1>(K1 key, V1 value)
        {
            throw new System.NotImplementedException();
        }
    }
}
