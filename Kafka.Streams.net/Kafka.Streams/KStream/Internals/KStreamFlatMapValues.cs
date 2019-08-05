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







class KStreamFlatMapValues<K, V, V1> : ProcessorSupplier<K, V> {

    private  ValueMapperWithKey<K, V, Iterable<V1>> mapper;

    KStreamFlatMapValues( ValueMapperWithKey<K, V, Iterable<V1>> mapper)
{
        this.mapper = mapper;
    }

    
    public Processor<K, V> get()
{
        return new KStreamFlatMapValuesProcessor();
    }

    private KStreamFlatMapValuesProcessor : AbstractProcessor<K, V> {
        
        public void process( K key,  V value)
{
             Iterable<V1> newValues = mapper.apply(key, value);
            foreach ( V1 v in newValues)
{
                context().forward(key, v);
            }
        }
    }
}
