
//using Kafka.Common.Utils;
//using Kafka.Streams.State.Interfaces;
//using System;
//using System.Collections;

//namespace Kafka.Streams.State.Internals
//{
//    public class FilteredCacheIterator : IPeekingKeyValueIterator<Bytes, LRUCacheEntry>
//    {
//        private IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator;
//        private HasNextCondition hasNextCondition;
//        private IPeekingKeyValueIterator<Bytes, LRUCacheEntry> wrappedIterator;

//        public KeyValuePair<Bytes, LRUCacheEntry> Current { get; }
//        object IEnumerator.Current { get; }

//        public FilteredCacheIterator(
//            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
//            HasNextCondition hasNextCondition,
//            ICacheFunction cacheFunction)
//        {
//            this.cacheIterator = cacheIterator;
//            this.hasNextCondition = hasNextCondition;
//            this.wrappedIterator = null; // new IPeekingKeyValueIterator<Bytes, LRUCacheEntry>()
//                                         //    {


//            //    public KeyValuePair<Bytes, LRUCacheEntry> peekNext()
//            //    {
//            //        return cachedPair(cacheIterator.peekNext());
//            //    }


//            //    public void close()
//            //    {
//            //        cacheIterator.close();
//            //    }


//            //    public Bytes peekNextKey()
//            //    {
//            //        return cacheFunction.key(cacheIterator.peekNextKey());
//            //    }


//            //    public bool hasNext()
//            //    {
//            //        return cacheIterator.hasNext();
//            //    }


//            //    public KeyValuePair<Bytes, LRUCacheEntry> next()
//            //    {
//            //        return cachedPair(cacheIterator.MoveNext());
//            //    }

//            //    private KeyValuePair<Bytes, LRUCacheEntry> cachedPair(KeyValuePair<Bytes, LRUCacheEntry> next)
//            //    {
//            //        return KeyValuePair.pair(cacheFunction.key(next.key), next.value);
//            //    }


//            //    public void Remove()
//            //    {
//            //        cacheIterator.Remove();
//            //    }
//            //};
//        }

//        public void close()
//        {
//            // no-op
//        }

//        public Bytes peekNextKey()
//        {
//            if (!hasNext())
//            {
//                throw new IndexOutOfRangeException();
//            }
//            return cacheIterator.peekNextKey();
//        }

//        public bool hasNext()
//        {
//            return hasNextCondition.hasNext(wrappedIterator);
//        }

//        public KeyValuePair<Bytes, LRUCacheEntry> MoveNext()
//        {
//            if (!hasNext())
//            {
//                throw new IndexOutOfRangeException();
//            }

//            return cacheIterator.peekNext();
//        }

//        public void Remove()
//        {
//            throw new InvalidOperationException();
//        }

//        public KeyValuePair<Bytes, LRUCacheEntry> peekNext()
//        {
//            if (!hasNext())
//            {
//                throw new IndexOutOfRangeException();
//            }
//            return cacheIterator.peekNext();
//        }

//        bool IEnumerator.MoveNext()
//        {
//            throw new NotImplementedException();
//        }

//        public void Reset()
//        {
//            throw new NotImplementedException();
//        }

//        public void Dispose()
//        {
//            throw new NotImplementedException();
//        }
//    }
//}