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







class KStreamFilter<K, V> : ProcessorSupplier<K, V> {

    private  Predicate<K, V> predicate;
    private  bool filterNot;

    public KStreamFilter( Predicate<K, V> predicate,  bool filterNot)
{
        this.predicate = predicate;
        this.filterNot = filterNot;
    }

    
    public Processor<K, V> get()
{
        return new KStreamFilterProcessor();
    }

    private KStreamFilterProcessor : AbstractProcessor<K, V> {
        
        public void process( K key,  V value)
{
            if (filterNot ^ predicate.test(key, value))
{
                context().forward(key, value);
            }
        }
    }
}
