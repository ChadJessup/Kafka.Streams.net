using Confluent.Kafka;
using Xunit;
using System;

namespace Kafka.Streams.Tests.State
{
    public class NoOpWindowStore : ReadOnlyWindowStore, StateStore
    {

        private static class EmptyWindowStoreIterator : WindowStoreIterator<KeyValue>
        {


            public void close()
            {
            }


            public long peekNextKey()
            {
                throw new NoSuchElementException();
            }


            public bool hasNext()
            {
                return false;
            }


            public KeyValuePair<long, KeyValue> next()
            {
                throw new NoSuchElementException();
            }


            public void remove()
            {
            }
        }

        private static WindowStoreIterator<KeyValue> EMPTY_WINDOW_STORE_ITERATOR = new EmptyWindowStoreIterator();


        public string name()
        {
            return "";
        }


        public void init(ProcessorContext context, StateStore root)
        {

        }


        public void flush()
        {

        }


        public void close()
        {

        }


        public bool persistent()
        {
            return false;
        }


        public bool isOpen()
        {
            return false;
        }


        public object fetch(object key, long time)
        {
            return null;
        }



        public WindowStoreIterator fetch(object key, long timeFrom, long timeTo)
        {
            return EMPTY_WINDOW_STORE_ITERATOR;
        }


        public WindowStoreIterator fetch(object key, Instant from, Instant to)
        {
            return EMPTY_WINDOW_STORE_ITERATOR;
        }



        public WindowStoreIterator<KeyValue> fetch(object from, object to, long timeFrom, long timeTo)
        {
            return EMPTY_WINDOW_STORE_ITERATOR;
        }


        public KeyValueIterator fetch(object from,
                                      object to,
                                      Instant fromTime,
                                      Instant toTime)
        {// throws IllegalArgumentException
            return EMPTY_WINDOW_STORE_ITERATOR;
        }


        public WindowStoreIterator<KeyValue> all()
        {
            return EMPTY_WINDOW_STORE_ITERATOR;
        }



        public WindowStoreIterator<KeyValue> fetchAll(long timeFrom, long timeTo)
        {
            return EMPTY_WINDOW_STORE_ITERATOR;
        }


        public KeyValueIterator fetchAll(Instant from, Instant to)
        {
            return EMPTY_WINDOW_STORE_ITERATOR;
        }
    }
}
