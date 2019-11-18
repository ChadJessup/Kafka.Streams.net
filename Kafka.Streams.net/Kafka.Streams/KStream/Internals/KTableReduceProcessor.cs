

//        public override void process(K key, Change<V> value)
//        {
//            // the keys should never be null
//            if (key == null)
//            {
//                throw new StreamsException("Record key for KTable reduce operator with state " + storeName + " should not be null.");
//            }

//            ValueAndTimestamp<V> oldAggAndTimestamp = store[key];
//            V oldAgg = getValueOrNull(oldAggAndTimestamp);
//            V intermediateAgg;
//            long newTimestamp;

//            // first try to Remove the old value
//            if (value.oldValue != null && oldAgg != null)
//            {
//                intermediateAgg = removeReducer.apply(oldAgg, value.oldValue);
//                newTimestamp = Math.Max(context.timestamp(), oldAggAndTimestamp.timestamp());
//            }
//            else
//            {
//                intermediateAgg = oldAgg;
//                newTimestamp = context.timestamp();
//            }

//            // then try to.Add the new value
//            V newAgg;
//            if (value.newValue != null)
//            {
//                if (intermediateAgg == null)
//                {
//                    newAgg = value.newValue;
//                }
//                else
//                {
//                    newAgg = addReducer.apply(intermediateAgg, value.newValue);
//                    newTimestamp = Math.Max(context.timestamp(), oldAggAndTimestamp.timestamp);
//                }
//            }
//            else
//            {
//                newAgg = intermediateAgg;
//            }

//            // update the store with the new value
//            store.Add(key, ValueAndTimestamp<V>.make(newAgg, newTimestamp));
//            tupleForwarder.maybeForward(key, newAgg, sendOldValues ? oldAgg : null, newTimestamp);
//        }
//    }
//}
