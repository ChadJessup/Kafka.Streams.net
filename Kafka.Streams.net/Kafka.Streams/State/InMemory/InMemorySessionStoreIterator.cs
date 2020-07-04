using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State.KeyValues;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public class InMemorySessionStoreIterator : IKeyValueIterator<IWindowed<Bytes>, byte[]>
    {
        private readonly IEnumerator<KeyValuePair<DateTime, Dictionary<Bytes, Dictionary<DateTime, byte[]>>>> endTimeIterator;
        private readonly IEnumerator<KeyValuePair<Bytes, Dictionary<DateTime, byte[]>>> keyIterator;
        private IEnumerator<KeyValuePair<DateTime, byte[]>>? recordIterator;

        private KeyValuePair<IWindowed<Bytes>, byte[]>? next;
        private Bytes currentKey;
        private DateTime currentEndTime;

        private readonly Bytes keyFrom;
        private readonly Bytes keyTo;
        private readonly DateTime latestSessionStartTime;

        private readonly IClosingCallback callback;

        public KeyValuePair<IWindowed<Bytes>, byte[]> Current { get; }
        object IEnumerator.Current { get; }

        public InMemorySessionStoreIterator(
            Bytes keyFrom,
            Bytes keyTo,
            DateTime latestSessionStartTime,
            IEnumerator<KeyValuePair<DateTime, Dictionary<Bytes, Dictionary<DateTime, byte[]>>>> endTimeIterator,
            IClosingCallback callback)
        {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.latestSessionStartTime = latestSessionStartTime;

            this.endTimeIterator = endTimeIterator;
            this.callback = callback;
            this.SetAllIterators();
        }


        public bool MoveNext()
        {
            if (this.next != null)
            {
                return true;
            }
            else if (this.recordIterator == null)
            {
                return false;
            }
            else
            {
                this.next = this.GetNext();
                return this.next != null;
            }
        }


        public IWindowed<Bytes> PeekNextKey()
        {
            if (!this.MoveNext())
            {
                throw new ArgumentOutOfRangeException();
            }

            return this.Next().Value.Key;
        }

        public KeyValuePair<IWindowed<Bytes>, byte[]>? Next()
        {
            if (!this.MoveNext())
            {
                throw new ArgumentOutOfRangeException();
            }

            KeyValuePair<IWindowed<Bytes>, byte[]>? ret = this.next;
            this.next = null;
            return ret;
        }

        public void Close()
        {
            this.next = null;
            this.recordIterator = null;
            this.callback.DeregisterIterator(this);
        }

        public DateTime MinTime()
        {
            return this.currentEndTime;
        }

        // getNext is only called when either recordIterator or segmentIterator has a next
        // Note this does not guarantee a next record exists as the next segments may not contain any keys in range
        private KeyValuePair<IWindowed<Bytes>, byte[]>? GetNext()
        {
            if (!this.recordIterator.MoveNext())
            {
                this.GetNextIterators();
            }

            if (this.recordIterator == null)
            {
                return null;
            }

            KeyValuePair<DateTime, byte[]> nextRecord = this.recordIterator.Current;
            SessionWindow sessionWindow = new SessionWindow(nextRecord.Key, this.currentEndTime);
            IWindowed<Bytes> windowedKey = new Windowed<Bytes>(this.currentKey, sessionWindow);

            return KeyValuePair.Create(windowedKey, nextRecord.Value);
        }

        // Called when the inner two (key and starttime) iterators are empty to roll to the next endTimestamp
        // Rolls All three iterators forward until recordIterator has a next entry
        // Sets recordIterator to null if there are no records to return
        private void SetAllIterators()
        {
            while (this.endTimeIterator.MoveNext())
            {
                var nextEndTimeEntry = this.endTimeIterator.Current;
                this.currentEndTime = nextEndTimeEntry.Key;
                //keyIterator = nextEndTimeEntry.Value.subMap(keyFrom, true, keyTo, true).GetEnumerator();

                if (this.SetInnerIterators())
                {
                    return;
                }
            }

            this.recordIterator = null;
        }

        // Rolls the inner two iterators (key and record) forward until recordIterators has a next entry
        // Returns false if no more records are found (for the current end time)
        private bool SetInnerIterators()
        {
            while (this.keyIterator.MoveNext())
            {
                KeyValuePair<Bytes, Dictionary<DateTime, byte[]>> nextKeyEntry = this.keyIterator.Current;
                this.currentKey = nextKeyEntry.Key;

                if (this.latestSessionStartTime == DateTime.MaxValue)
                {
                    this.recordIterator = nextKeyEntry.Value.GetEnumerator();
                }
                else
                {
                    this.recordIterator = null;// nextKeyEntry.Value.headMap(latestSessionStartTime, true).iterator();
                }

                if (this.recordIterator.MoveNext())
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
            if (this.SetInnerIterators())
            {
                return;
            }

            this.SetAllIterators();
        }

        public void Reset()
        {
        }

        public void Dispose()
        {
        }
    }
}
