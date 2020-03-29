using Confluent.Kafka;
using Xunit;
using System;

namespace Kafka.Streams.Tests.State
{
    public class NoOpWindowStore : ReadOnlyWindowStore, StateStore
    {

        private static class EmptyWindowStoreIterator : WindowStoreIterator<KeyValue>
        {


            public void Close()
            {
            }


            public long PeekNextKey()
            {
                throw new NoSuchElementException();
            }


            public bool HasNext()
            {
                return false;
            }


            public KeyValuePair<long, KeyValue> Next()
            {
                throw new NoSuchElementException();
            }


            public void Remove()
            {
            }
        }

        private static WindowStoreIterator<KeyValue> EMPTY_WINDOW_STORE_ITERATOR = new EmptyWindowStoreIterator();


        public string Name()
        {
            return "";
        }


        public void Init(ProcessorContext context, StateStore root)
        {

        }


        public void Flush()
        {

        }


        public void Close()
        {

        }


        public bool Persistent()
        {
            return false;
        }


        public bool IsOpen()
        {
            return false;
        }


        public object Fetch(object key, long time)
        {
            return null;
        }



        public WindowStoreIterator Fetch(object key, long timeFrom, long timeTo)
        {
            return EMPTY_WINDOW_STORE_ITERATOR;
        }


        public WindowStoreIterator Fetch(object key, Instant from, Instant to)
        {
            return EMPTY_WINDOW_STORE_ITERATOR;
        }



        public WindowStoreIterator<KeyValue> Fetch(object from, object to, long timeFrom, long timeTo)
        {
            return EMPTY_WINDOW_STORE_ITERATOR;
        }


        public KeyValueIterator Fetch(object from,
                                      object to,
                                      Instant fromTime,
                                      Instant toTime)
        {// throws IllegalArgumentException
            return EMPTY_WINDOW_STORE_ITERATOR;
        }


        public WindowStoreIterator<KeyValue> All()
        {
            return EMPTY_WINDOW_STORE_ITERATOR;
        }



        public WindowStoreIterator<KeyValue> FetchAll(long timeFrom, long timeTo)
        {
            return EMPTY_WINDOW_STORE_ITERATOR;
        }


        public KeyValueIterator FetchAll(Instant from, Instant to)
        {
            return EMPTY_WINDOW_STORE_ITERATOR;
        }
    }
}
