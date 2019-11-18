

//        public void process(K key, Change<V> value)
//        {
//            // the keys should never be null
//            if (key == null)
//            {
//                throw new StreamsException("Record key for KTable aggregate operator with state " + storeName + " should not be null.");
//            }

//            ValueAndTimestamp<T> oldAggAndTimestamp = store[key];
//            T oldAgg = getValueOrNull(oldAggAndTimestamp);
//            T intermediateAgg;
//            long newTimestamp = context.timestamp();

//            // first try to Remove the old value
//            if (value.oldValue != null && oldAgg != null)
//            {
//                intermediateAgg = Remove.apply(key, value.oldValue, oldAgg);
//                newTimestamp = Math.Max(context.timestamp(), oldAggAndTimestamp.timestamp());
//            }
//            else
//            {
//                intermediateAgg = oldAgg;
//            }

//            // then try to.Add the new value
//            T newAgg;
//            if (value.newValue != null)
//            {
//                T initializedAgg;
//                if (intermediateAgg == null)
//                {
//                    initializedAgg = initializer.apply();
//                }
//                else
//                {

//                    initializedAgg = intermediateAgg;
//                }

//                newAgg = add.apply(key, value.newValue, initializedAgg);
//                if (oldAggAndTimestamp != null)
//                {
//                    newTimestamp = Math.Max(context.timestamp(), oldAggAndTimestamp.timestamp());
//                }
//            }
//            else
//            {

//                newAgg = intermediateAgg;
//            }

//            // update the store with the new value
//            store.Add(key, ValueAndTimestamp.make(newAgg, newTimestamp));
//            tupleForwarder.maybeForward(key, newAgg, sendOldValues ? oldAgg : null, newTimestamp);
//        }

//    }
//}
