using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State.KeyValues;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public class InMemorySessionStoreIterator : IKeyValueIterator<Windowed<Bytes>, byte[]>
    {
        private IEnumerator<KeyValuePair<long, Dictionary<Bytes, Dictionary<long, byte[]>>>> endTimeIterator;
        private IEnumerator<KeyValuePair<Bytes, Dictionary<long, byte[]>>> keyIterator;
        private IEnumerator<KeyValuePair<long, byte[]>>? recordIterator;

        private KeyValuePair<Windowed<Bytes>, byte[]>? next;
        private Bytes currentKey;
        private long currentEndTime;

        private Bytes keyFrom;
        private Bytes keyTo;
        private long latestSessionStartTime;

        private IClosingCallback callback;

        public KeyValuePair<Windowed<Bytes>, byte[]> Current { get; }
        object IEnumerator.Current { get; }

        public InMemorySessionStoreIterator(
            Bytes keyFrom,
            Bytes keyTo,
            long latestSessionStartTime,
            IEnumerator<KeyValuePair<long, Dictionary<Bytes, Dictionary<long, byte[]>>>> endTimeIterator,
            IClosingCallback callback)
        {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.latestSessionStartTime = latestSessionStartTime;

            this.endTimeIterator = endTimeIterator;
            this.callback = callback;
            SetAllIterators();
        }


        public bool MoveNext()
        {
            if (next != null)
            {
                return true;
            }
            else if (recordIterator == null)
            {
                return false;
            }
            else
            {
                next = GetNext();
                return next != null;
            }
        }


        public Windowed<Bytes> PeekNextKey()
        {
            if (!this.MoveNext())
            {
                throw new ArgumentOutOfRangeException();
            }

            return this.Next().Value.Key;
        }

        public KeyValuePair<Windowed<Bytes>, byte[]>? Next()
        {
            if (!this.MoveNext())
            {
                throw new ArgumentOutOfRangeException();
            }

            KeyValuePair<Windowed<Bytes>, byte[]>? ret = next;
            next = null;
            return ret;
        }

        public void Close()
        {
            next = null;
            recordIterator = null;
            callback.DeregisterIterator(this);
        }

        public long MinTime()
        {
            return currentEndTime;
        }

        // getNext is only called when either recordIterator or segmentIterator has a next
        // Note this does not guarantee a next record exists as the next segments may not contain any keys in range
        private KeyValuePair<Windowed<Bytes>, byte[]>? GetNext()
        {
            if (!recordIterator.MoveNext())
            {
                GetNextIterators();
            }

            if (recordIterator == null)
            {
                return null;
            }

            KeyValuePair<long, byte[]> nextRecord = recordIterator.Current;
            SessionWindow sessionWindow = new SessionWindow(nextRecord.Key, currentEndTime);
            Windowed<Bytes> windowedKey = new Windowed<Bytes>(currentKey, sessionWindow);

            return KeyValuePair.Create(windowedKey, nextRecord.Value);
        }

        // Called when the inner two (key and starttime) iterators are empty to roll to the next endTimestamp
        // Rolls all three iterators forward until recordIterator has a next entry
        // Sets recordIterator to null if there are no records to return
        private void SetAllIterators()
        {
            while (endTimeIterator.MoveNext())
            {
                var nextEndTimeEntry = endTimeIterator.Current;
                currentEndTime = nextEndTimeEntry.Key;
                //keyIterator = nextEndTimeEntry.Value.subMap(keyFrom, true, keyTo, true).GetEnumerator();

                if (SetInnerIterators())
                {
                    return;
                }
            }

            recordIterator = null;
        }

        // Rolls the inner two iterators (key and record) forward until recordIterators has a next entry
        // Returns false if no more records are found (for the current end time)
        private bool SetInnerIterators()
        {
            while (keyIterator.MoveNext())
            {
                KeyValuePair<Bytes, Dictionary<long, byte[]>> nextKeyEntry = keyIterator.Current;
                currentKey = nextKeyEntry.Key;

                if (latestSessionStartTime == long.MaxValue)
                {
                    recordIterator = nextKeyEntry.Value.GetEnumerator();
                }
                else
                {
                    recordIterator = null;// nextKeyEntry.Value.headMap(latestSessionStartTime, true).iterator();
                }

                if (recordIterator.MoveNext())
                {
                    return true;
                }
            }

            return false;
        }

        // Called when the current recordIterator has no entries left to roll it to the next valid entry
        // When there are no more records to return, recordIterator will be set to null
        private void GetNextIterators()
        {
            if (SetInnerIterators())
            {
                return;
            }

            SetAllIterators();
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
