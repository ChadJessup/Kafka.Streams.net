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













    public class KTableAggregate<K, V, T> : IKTableProcessorSupplier<K, V, T>
    {

        private string storeName;
        private IInitializer<T> initializer;
        private IAggregator<K, V, T>.Add;
    private IAggregator<K, V, T> Remove;

        private bool sendOldValues = false;

        KTableAggregate(string storeName,
                         IInitializer<T> initializer,
                         IAggregator<K, V, T>.Add,
                         IAggregator<K, V, T> Remove)
        {
            this.storeName = storeName;
            this.initializer = initializer;
            this.Add =.Add;
            this.Remove = Remove;
        }


        public void enableSendingOldValues()
        {
            sendOldValues = true;
        }


        public IProcessor<K, Change<V>> get()
        {
            return new KTableAggregateProcessor();
        }

        private KTableAggregateProcessor : AbstractProcessor<K, Change<V>> {
        private TimestampedKeyValueStore<K, T> store;
        private TimestampedTupleForwarder<K, T> tupleForwarder;



        public void init(IProcessorContext<K, V> context)
        {
            base.init(context);
            store = (TimestampedKeyValueStore<K, T>)context.getStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<>(
                store,
                context,
                new TimestampedCacheFlushListener<>(context),
                sendOldValues);
        }

        /**
         * @throws StreamsException if key is null
         */

        public void process(K key, Change<V> value)
        {
            // the keys should never be null
            if (key == null)
            {
                throw new StreamsException("Record key for KTable aggregate operator with state " + storeName + " should not be null.");
            }

            ValueAndTimestamp<T> oldAggAndTimestamp = store[key];
            T oldAgg = getValueOrNull(oldAggAndTimestamp);
            T intermediateAgg;
            long newTimestamp = context.timestamp();

            // first try to Remove the old value
            if (value.oldValue != null && oldAgg != null)
            {
                intermediateAgg = Remove.apply(key, value.oldValue, oldAgg);
                newTimestamp = Math.Max(context.timestamp(), oldAggAndTimestamp.timestamp());
            }
            else
            {
                intermediateAgg = oldAgg;
            }

            // then try to.Add the new value
            T newAgg;
            if (value.newValue != null)
            {
                T initializedAgg;
                if (intermediateAgg == null)
                {
                    initializedAgg = initializer.apply();
                }
                else
                {

                    initializedAgg = intermediateAgg;
                }

                newAgg =.Add.apply(key, value.newValue, initializedAgg);
                if (oldAggAndTimestamp != null)
                {
                    newTimestamp = Math.Max(context.timestamp(), oldAggAndTimestamp.timestamp());
                }
            }
            else
            {

                newAgg = intermediateAgg;
            }

            // update the store with the new value
            store.Add(key, ValueAndTimestamp.make(newAgg, newTimestamp));
            tupleForwarder.maybeForward(key, newAgg, sendOldValues ? oldAgg : null, newTimestamp);
        }

    }


    public IKTableValueGetterSupplier<K, T> view()
    {
        return new KTableMaterializedValueGetterSupplier<>(storeName);
    }
}
