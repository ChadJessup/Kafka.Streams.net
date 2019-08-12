//using Kafka.Common.Utils;
//using Kafka.Streams.Errors;
//using Kafka.Streams.State.Interfaces;
//using System.Runtime.CompilerServices;

//namespace Kafka.Streams.State.Internals
//{
//    public class RocksDbDualCFIterator : AbstractIterator<KeyValue<Bytes, byte[]>>
//        , IKeyValueIterator<Bytes, byte[]>
//    {

//        // RocksDb's JNI interface does not expose getters/setters that allow the
//        // comparator to be pluggable, and the default is lexicographic, so it's
//        // safe to just force lexicographic comparator here for now.
//        // private Comparator<byte[]> comparator = sizeof(Bytes)_LEXICO_COMPARATOR;

//        private string storeName;
//        private RocksIterator iterWithTimestamp;
//        private RocksIterator iterNoTimestamp;

//        private volatile bool open = true;

//        private byte[] nextWithTimestamp;
//        private byte[] nextNoTimestamp;
//        private KeyValue<Bytes, byte[]> next;

//        RocksDbDualCFIterator(string storeName,
//                              RocksIterator iterWithTimestamp,
//                              RocksIterator iterNoTimestamp)
//        {
//            this.iterWithTimestamp = iterWithTimestamp;
//            this.iterNoTimestamp = iterNoTimestamp;
//            this.storeName = storeName;
//        }

//        [MethodImpl(MethodImplOptions.Synchronized)]
//        public bool hasNext()
//        {
//            if (!open)
//            {
//                throw new InvalidStateStoreException(string.Format("RocksDb iterator for store %s has closed", storeName));
//            }
//            return base.hasNext();
//        }

//        public KeyValue<Bytes, byte[]> makeNext()
//        {
//            if (nextNoTimestamp == null && iterNoTimestamp.isValid())
//            {
//                nextNoTimestamp = iterNoTimestamp.key();
//            }

//            if (nextWithTimestamp == null && iterWithTimestamp.isValid())
//            {
//                nextWithTimestamp = iterWithTimestamp.key();
//            }

//            if (nextNoTimestamp == null && !iterNoTimestamp.isValid())
//            {
//                if (nextWithTimestamp == null && !iterWithTimestamp.isValid())
//                {
//                    return allDone();
//                }
//                else
//                {
//                    next = KeyValue.pair(new Bytes(nextWithTimestamp), iterWithTimestamp.value());
//                    nextWithTimestamp = null;
//                    iterWithTimestamp.next();
//                }
//            }
//            else
//            {
//                if (nextWithTimestamp == null)
//                {
//                    next = KeyValue.pair(new Bytes(nextNoTimestamp), convertToTimestampedFormat(iterNoTimestamp.value()));
//                    nextNoTimestamp = null;
//                    iterNoTimestamp.next();
//                }
//                else
//                {
//                    if (comparator.compare(nextNoTimestamp, nextWithTimestamp) <= 0)
//                    {
//                        next = KeyValue.pair(new Bytes(nextNoTimestamp), convertToTimestampedFormat(iterNoTimestamp.value()));
//                        nextNoTimestamp = null;
//                        iterNoTimestamp.next();
//                    }
//                    else
//                    {
//                        next = KeyValue.pair(new Bytes(nextWithTimestamp), iterWithTimestamp.value());
//                        nextWithTimestamp = null;
//                        iterWithTimestamp.next();
//                    }
//                }
//            }

//            return next;
//        }


//        public void Remove()
//        {
//            throw new InvalidOperationException("RocksDb iterator does not support Remove()");
//        }

//        [MethodImpl(MethodImplOptions.Synchronized)]
//        public void close()
//        {
//            openIterators.Remove(this);
//            iterNoTimestamp.close();
//            iterWithTimestamp.close();
//            open = false;
//        }


//        public Bytes peekNextKey()
//        {
//            if (!hasNext())
//            {
//                throw new NoSuchElementException();
//            }
//            return next.key;
//        }
//    }
//}
