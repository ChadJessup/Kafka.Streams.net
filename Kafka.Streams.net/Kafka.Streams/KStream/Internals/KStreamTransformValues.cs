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
namespace Kafka.Streams.KStream.Internals
{









public KStreamTransformValues<K, V, R> : ProcessorSupplier<K, V> {

    private  ValueTransformerWithKeySupplier<K, V, R> valueTransformerSupplier;

    KStreamTransformValues( ValueTransformerWithKeySupplier<K, V, R> valueTransformerSupplier)
{
        this.valueTransformerSupplier = valueTransformerSupplier;
    }

    
    public Processor<K, V> get()
{
        return new KStreamTransformValuesProcessor<>(valueTransformerSupplier());
    }

    public static KStreamTransformValuesProcessor<K, V, R> : Processor<K, V> {

        private  ValueTransformerWithKey<K, V, R> valueTransformer;
        private IProcessorContext context;

        KStreamTransformValuesProcessor( ValueTransformerWithKey<K, V, R> valueTransformer)
{
            this.valueTransformer = valueTransformer;
        }

        
        public void init( IProcessorContext context)
{
            valueTransformer.init(new ForwardingDisabledProcessorContext(context));
            this.context = context;
        }

        
        public void process( K key,  V value)
{
            context.forward(key, valueTransformer.transform(key, value));
        }

        
        public void close()
{
            valueTransformer.close();
        }
    }
}
