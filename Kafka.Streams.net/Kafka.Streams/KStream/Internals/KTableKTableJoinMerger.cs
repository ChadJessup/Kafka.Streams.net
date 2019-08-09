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
using Kafka.Streams.Processor;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableKTableJoinMerger<K, V> : IKTableProcessorSupplier<K, V, V>
    {
        private IKTableProcessorSupplier<K, object, V> parent1;
        private IKTableProcessorSupplier<K, object, V> parent2;
        private string queryableName;
        private bool sendOldValues = false;

        KTableKTableJoinMerger(
            IKTableProcessorSupplier<K, object, V> parent1,
            IKTableProcessorSupplier<K, object, V> parent2,
            string queryableName)
        {
            this.parent1 = parent1;
            this.parent2 = parent2;
            this.queryableName = queryableName;
        }

        public string getQueryableName()
        {
            return queryableName;
        }


        public IProcessor<K, Change<V>> get()
        {
            return new KTableKTableJoinMergeProcessor();
        }


        public IKTableValueGetterSupplier<K, V> view()
        {
            // if the result KTable is materialized, use the materialized store to return getter value;
            // otherwise rely on the parent getter and apply join on-the-fly
            if (queryableName != null)
            {
                return new KTableMaterializedValueGetterSupplier<>(queryableName);
            }
            else
            {

                //    return new KTableValueGetterSupplier<K, V>()
                //    {

                //    public KTableValueGetter<K, V> get()
                //    {
                //        return parent1.view()[];
                //    }


                //    public string[] storeNames()
                //    {
                //        string[] storeNames1 = parent1.view().storeNames();
                //        string[] storeNames2 = parent2.view().storeNames();
                //        HashSet<string> stores = new HashSet<>(storeNames1.Length + storeNames2.Length);
                //        Collections.AddAll(stores, storeNames1);
                //        Collections.AddAll(stores, storeNames2);
                //        return stores.ToArray(new string[stores.size()]);
                //    }
                //};
            }
        }


        public void enableSendingOldValues()
        {
            parent1.enableSendingOldValues();
            parent2.enableSendingOldValues();
            sendOldValues = true;
        }

        public static KTableKTableJoinMerger<K, V> of(IKTableProcessorSupplier<K, ?, V> parent1,
                                                              IKTableProcessorSupplier<K, ?, V> parent2)
        {
            return of(parent1, parent2, null);
        }

        public static KTableKTableJoinMerger<K, V> of(IKTableProcessorSupplier<K, object, V> parent1,
                                                              IKTableProcessorSupplier<K, object, V> parent2,
                                                              string queryableName)
        {
            return new KTableKTableJoinMerger<K, V>(parent1, parent2, queryableName);
        }
    }
}