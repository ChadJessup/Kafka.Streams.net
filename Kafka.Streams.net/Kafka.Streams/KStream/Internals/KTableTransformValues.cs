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
    public class KTableTransformValues<K, V, V1> : IKTableProcessorSupplier<K, V, V1>
    {
        private KTableImpl<K, object, V> parent;
        private IValueTransformerWithKeySupplier<K, V, V1> transformerSupplier;
        private string queryableName;
        private bool sendOldValues = false;

        public KTableTransformValues(
            KTableImpl<K, object, V> parent,
            IValueTransformerWithKeySupplier<K, V, V1> transformerSupplier,
            string queryableName)
        {
            this.parent = parent ?? throw new System.ArgumentNullException("parent", nameof(parent));
            this.transformerSupplier = transformerSupplier = transformerSupplier ?? throw new System.ArgumentNullException("transformerSupplier", nameof(transformerSupplier));
            this.queryableName = queryableName;
        }

        public IProcessor<K, Change<V>> get()
        {
            return new KTableTransformValuesProcessor(transformerSupplier());
        }


        public IKTableValueGetterSupplier<K, V1> view()
        {
            if (queryableName != null)
            {
                return new KTableMaterializedValueGetterSupplier<K, V1>(queryableName);
            }

            return null;
            //            return new IKTableValueGetterSupplier<K, V1>()
            //{
            //             IKTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

            //            public IKTableValueGetter<K, V1> get()
            //            {
            //                return new KTableTransformValuesGetter<K, V, V1>(
            //                    parentValueGetterSupplier[],
            //                    transformerSupplier());
            //            }


            //            public string[] storeNames()
            //            {
            //                return parentValueGetterSupplier.storeNames();
            //            }
            //        };
            //    }


            //    public void enableSendingOldValues()
            //    {
            //        parent.enableSendingOldValues();
            //        sendOldValues = true;
            //    }
        }
    }
}