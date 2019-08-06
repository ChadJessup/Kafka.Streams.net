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
    class KTableTransformValues<K, V, V1> : IKTableProcessorSupplier<K, V, V1>
    {
        private KTableImpl<K, object, V> parent;
        private ValueTransformerWithKeySupplier<K, V, V1> transformerSupplier;
        private string queryableName;
        private bool sendOldValues = false;

        KTableTransformValues(KTableImpl<K, object, V> parent,
                               ValueTransformerWithKeySupplier<K, V, V1> transformerSupplier,
                               string queryableName)
        {
            this.parent = parent ?? throw new System.ArgumentNullException("parent", nameof(parent));
            this.transformerSupplier = transformerSupplier = transformerSupplier ?? throw new System.ArgumentNullException("transformerSupplier", nameof(transformerSupplier));
            this.queryableName = queryableName;
        }


        public Processor<K, Change<V>> get()
        {
            return new KTableTransformValuesProcessor(transformerSupplier());
        }


        public KTableValueGetterSupplier<K, V1> view()
        {
            if (queryableName != null)
            {
                return new KTableMaterializedValueGetterSupplier<>(queryableName);
            }

            return new KTableValueGetterSupplier<K, V1>()
{
             KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

            public KTableValueGetter<K, V1> get()
            {
                return new KTableTransformValuesGetter(
                    parentValueGetterSupplier[],
                    transformerSupplier());
            }


            public string[] storeNames()
            {
                return parentValueGetterSupplier.storeNames();
            }
        };
    }


    public void enableSendingOldValues()
    {
        parent.enableSendingOldValues();
        sendOldValues = true;
    }


    private class KTableTransformValuesProcessor : AbstractProcessor<K, Change<V>>
    {
        private ValueTransformerWithKey<K, V, V1> valueTransformer;
        private TimestampedKeyValueStore<K, V1> store;
        private TimestampedTupleForwarder<K, V1> tupleForwarder;

        private KTableTransformValuesProcessor(ValueTransformerWithKey<K, V, V1> valueTransformer)
        {
            this.valueTransformer = valueTransformer = valueTransformer ?? throw new System.ArgumentNullException("valueTransformer", nameof(valueTransformer));
        }



        public void init(IProcessorContext context)
        {
            base.init(context);
            valueTransformer.init(new ForwardingDisabledProcessorContext(context));
            if (queryableName != null)
            {
                store = (TimestampedKeyValueStore<K, V1>)context.getStateStore(queryableName);
                tupleForwarder = new TimestampedTupleForwarder<>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<>(context),
                    sendOldValues);
            }
        }


        public void process(K key, Change<V> change)
        {
            V1 newValue = valueTransformer.transform(key, change.newValue);

            if (queryableName == null)
            {
                V1 oldValue = sendOldValues ? valueTransformer.transform(key, change.oldValue) : null;
                context().forward(key, new Change<>(newValue, oldValue));
            }
            else
            {

                V1 oldValue = sendOldValues ? getValueOrNull(store[key]] : null;
                store.Add(key, ValueAndTimestamp.make(newValue, context().timestamp()));
                tupleForwarder.maybeForward(key, newValue, oldValue);
            }
        }


        public void close()
        {
            valueTransformer.close();
        }
    }


    private class KTableTransformValuesGetter : KTableValueGetter<K, V1>
    {
        private KTableValueGetter<K, V> parentGetter;
        private ValueTransformerWithKey<K, V, V1> valueTransformer;

        KTableTransformValuesGetter(KTableValueGetter<K, V> parentGetter,
                                     ValueTransformerWithKey<K, V, V1> valueTransformer)
        {
            this.parentGetter = parentGetter = parentGetter ?? throw new System.ArgumentNullException("parentGetter", nameof(parentGetter));
            this.valueTransformer = valueTransformer = valueTransformer ?? throw new System.ArgumentNullException("valueTransformer", nameof(valueTransformer));
        }


        public void init(IProcessorContext context)
        {
            parentGetter.init(context);
            valueTransformer.init(new ForwardingDisabledProcessorContext(context));
        }


        public ValueAndTimestamp<V1> get(K key)
        {
            ValueAndTimestamp<V> valueAndTimestamp = parentGetter[key];
            return ValueAndTimestamp.make(
                valueTransformer.transform(key, getValueOrNull(valueAndTimestamp)),
                valueAndTimestamp == null ? UNKNOWN : valueAndTimestamp.timestamp());
        }


        public void close()
        {
            parentGetter.close();
            valueTransformer.close();
        }
    }
}
