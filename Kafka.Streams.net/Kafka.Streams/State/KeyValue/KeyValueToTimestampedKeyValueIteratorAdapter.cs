
//    public class KeyValueToTimestampedKeyValueIteratorAdapter<K> : IKeyValueIterator<K, byte[]>
//    {
//        private IKeyValueIterator<K, byte[]> innerIterator;

//        KeyValueToTimestampedKeyValueIteratorAdapter(IKeyValueIterator<K, byte[]> innerIterator)
//        {
//            this.innerIterator = innerIterator;
//        }

//        public void close()
//        {
//            innerIterator.close();
//        }

//        public K peekNextKey()
//        {
//            return innerIterator.peekNextKey();
//        }

//        public bool hasNext()
//        {
//            return innerIterator.hasNext();
//        }

//        public KeyValue<K, byte[]> next()
//        {
//            KeyValue<K, byte[]> plainKeyValue = innerIterator.next();
//            return KeyValue<K, byte[]>.pair(plainKeyValue.key, convertToTimestampedFormat(plainKeyValue.value));
//        }
//    }
//}