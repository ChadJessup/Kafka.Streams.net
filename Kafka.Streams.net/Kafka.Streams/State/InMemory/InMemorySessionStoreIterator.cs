

//using Kafka.Common.Utils;
//using Kafka.Streams.State.Interfaces;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    public class InMemorySessionStoreIterator : IKeyValueIterator<Windowed<Bytes>, byte[]>
//    {

//        private IEnumerator<Entry<long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<long, byte[]>>>> endTimeIterator;
//        private IEnumerator<Entry<Bytes, ConcurrentNavigableMap<long, byte[]>>> keyIterator;
//        private IEnumerator<Entry<long, byte[]>> recordIterator;

//        private KeyValuePair<Windowed<Bytes>, byte[]> next;
//        private Bytes currentKey;
//        private long currentEndTime;

//        private Bytes keyFrom;
//        private Bytes keyTo;
//        private long latestSessionStartTime;

//        private ClosingCallback callback;

//        InMemorySessionStoreIterator(Bytes keyFrom,
//                                     Bytes keyTo,
//                                     long latestSessionStartTime,
//                                     IEnumerator<Entry<long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<long, byte[]>>>> endTimeIterator,
//                                     ClosingCallback callback)
//        {
//            this.keyFrom = keyFrom;
//            this.keyTo = keyTo;
//            this.latestSessionStartTime = latestSessionStartTime;

//            this.endTimeIterator = endTimeIterator;
//            this.callback = callback;
//            setAllIterators();
//        }


//        public bool hasNext()
//        {
//            if (next != null)
//            {
//                return true;
//            }
//            else if (recordIterator == null)
//            {
//                return false;
//            }
//            else
//            {
//                next = getNext();
//                return next != null;
//            }
//        }


//        public Windowed<Bytes> peekNextKey()
//        {
//            if (!hasNext())
//            {
//                throw new NoSuchElementException();
//            }
//            return next.key;
//        }


//        public KeyValuePair<Windowed<Bytes>, byte[]> next()
//        {
//            if (!hasNext())
//            {
//                throw new NoSuchElementException();
//            }

//            KeyValuePair<Windowed<Bytes>, byte[]> ret = next;
//            next = null;
//            return ret;
//        }


//        public void close()
//        {
//            next = null;
//            recordIterator = null;
//            callback.deregisterIterator(this);
//        }

//        public long minTime()
//        {
//            return currentEndTime;
//        }

//        // getNext is only called when either recordIterator or segmentIterator has a next
//        // Note this does not guarantee a next record exists as the next segments may not contain any keys in range
//        private KeyValuePair<Windowed<Bytes>, byte[]> getNext()
//        {
//            if (!recordIterator.hasNext())
//            {
//                getNextIterators();
//            }

//            if (recordIterator == null)
//            {
//                return null;
//            }

//            KeyValuePair<long, byte[]> nextRecord = recordIterator.next();
//            SessionWindow sessionWindow = new SessionWindow(nextRecord.Key, currentEndTime);
//            Windowed<Bytes> windowedKey = new Windowed<>(currentKey, sessionWindow);

//            return new KeyValuePair<>(windowedKey, nextRecord.Value);
//        }

//        // Called when the inner two (key and starttime) iterators are empty to roll to the next endTimestamp
//        // Rolls all three iterators forward until recordIterator has a next entry
//        // Sets recordIterator to null if there are no records to return
//        private void setAllIterators()
//        {
//            while (endTimeIterator.hasNext())
//            {
//                Entry<long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<long, byte[]>>> nextEndTimeEntry = endTimeIterator.next();
//                currentEndTime = nextEndTimeEntry.Key;
//                keyIterator = nextEndTimeEntry.Value.subMap(keyFrom, true, keyTo, true).iterator();

//                if (setInnerIterators())
//                {
//                    return;
//                }
//            }
//            recordIterator = null;
//        }

//        // Rolls the inner two iterators (key and record) forward until recordIterators has a next entry
//        // Returns false if no more records are found (for the current end time)
//        private bool setInnerIterators()
//        {
//            while (keyIterator.hasNext())
//            {
//                Entry<Bytes, ConcurrentNavigableMap<long, byte[]>> nextKeyEntry = keyIterator.next();
//                currentKey = nextKeyEntry.Key;

//                if (latestSessionStartTime == long.MaxValue)
//                {
//                    recordIterator = nextKeyEntry.Value.iterator();
//                }
//                else
//                {
//                    recordIterator = nextKeyEntry.Value.headMap(latestSessionStartTime, true).iterator();
//                }

//                if (recordIterator.hasNext())
//                {
//                    return true;
//                }
//            }
//            return false;
//        }

//        // Called when the current recordIterator has no entries left to roll it to the next valid entry
//        // When there are no more records to return, recordIterator will be set to null
//        private void getNextIterators()
//        {
//            if (setInnerIterators())
//            {
//                return;
//            }

//            setAllIterators();
//        }
//    }
//}
