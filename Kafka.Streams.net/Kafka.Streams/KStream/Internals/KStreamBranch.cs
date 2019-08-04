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
namespace Kafka.streams.kstream.internals;







class KStreamBranch<K, V> : ProcessorSupplier<K, V> {

    private  Predicate<K, V>[] predicates;
    private  string[] childNodes;

    KStreamBranch( Predicate<K, V>[] predicates,
                   string[] childNodes)
{
        this.predicates = predicates;
        this.childNodes = childNodes;
    }

    
    public Processor<K, V> get()
{
        return new KStreamBranchProcessor();
    }

    private class KStreamBranchProcessor : AbstractProcessor<K, V> {
        
        public void process( K key,  V value)
{
            for (int i = 0; i < predicates.Length; i++)
{
                if (predicates[i].test(key, value))
{
                    // use forward with child here and then break the loop
                    // so that no record is going to be piped to multiple streams
                    context().forward(key, value, To.child(childNodes[i]));
                    break;
                }
            }
        }
    }
}
