//using Confluent.Kafka;
//using Xunit;
//using System;
//using NodaTime;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.State.Windowed;
//using System.Collections.Generic;
//using Kafka.Streams.State.ReadOnly;
//using Kafka.Streams.State;
//using Kafka.Streams.Processors.Interfaces;

//namespace Kafka.Streams.Tests.State
//{
//    public class NoOpWindowStore : IReadOnlyWindowStore, IStateStore
//    {

//        private static class EmptyWindowStoreIterator : IWindowStoreIterator<KeyValuePair>
//        {


//            public void Close()
//            {
//            }


//            public long PeekNextKey()
//            {
//                throw new NoSuchElementException();
//            }


//            public bool HasNext()
//            {
//                return false;
//            }


//            public KeyValuePair<long, KeyValuePair> Next()
//            {
//                throw new NoSuchElementException();
//            }


//            public void Remove()
//            {
//            }
//        }

//        private static IWindowStoreIterator<KeyValuePair> EMPTY_WINDOW_STORE_ITERATOR = new EmptyWindowStoreIterator();


//        public string Name()
//        {
//            return "";
//        }


//        public void Init(IProcessorContext context, IStateStore root)
//        {

//        }


//        public void Flush()
//        {

//        }


//        public void Close()
//        {

//        }


//        public bool Persistent()
//        {
//            return false;
//        }


//        public bool IsOpen()
//        {
//            return false;
//        }


//        public object Fetch(object key, long time)
//        {
//            return null;
//        }



//        public IWindowStoreIterator Fetch(object key, long timeFrom, long timeTo)
//        {
//            return EMPTY_WINDOW_STORE_ITERATOR;
//        }


//        public IWindowStoreIterator Fetch(object key, Instant from, Instant to)
//        {
//            return EMPTY_WINDOW_STORE_ITERATOR;
//        }



//        public IWindowStoreIterator<KeyValuePair> Fetch(object from, object to, long timeFrom, long timeTo)
//        {
//            return EMPTY_WINDOW_STORE_ITERATOR;
//        }


//        public IKeyValueIterator Fetch(object from,
//                                      object to,
//                                      Instant fromTime,
//                                      Instant toTime)
//        {// throws ArgumentException
//            return EMPTY_WINDOW_STORE_ITERATOR;
//        }


//        public IWindowStoreIterator<KeyValuePair> All()
//        {
//            return EMPTY_WINDOW_STORE_ITERATOR;
//        }



//        public IWindowStoreIterator<KeyValuePair> FetchAll(long timeFrom, long timeTo)
//        {
//            return EMPTY_WINDOW_STORE_ITERATOR;
//        }


//        public IKeyValueIterator FetchAll(Instant from, Instant to)
//        {
//            return EMPTY_WINDOW_STORE_ITERATOR;
//        }
//    }
//}
