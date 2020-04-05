
//        static TimeWindow timeWindowForSize(long startMs,
//                                            long windowSize)
//        {
//            long endMs = startMs + windowSize;

//            if (endMs < 0)
//            {
//                LOG.LogWarning("Warning: window end time was truncated to long.MAX");
//                endMs = long.MaxValue;
//            }
//            return new TimeWindow(startMs, endMs);
//        }

//        // for pipe serdes

//        public static byte[] toBinary<K>(Windowed<K> timeKey,
//                                          ISerializer<K> serializer,
//                                          string topic)
//        {
//            byte[] bytes = serializer.Serialize(topic, timeKey.key);
//            ByteBuffer buf = new ByteBuffer().Allocate(bytes.Length + TIMESTAMP_SIZE);
//            buf.Add(bytes);
//            buf.putLong(timeKey.window.start());

//            return buf.array();
//        }

//        public static Windowed<K> from<K>(byte[] binaryKey,
//                                           long windowSize,
//                                           IDeserializer<K> deserializer,
//                                           string topic)
//        {
//            byte[] bytes = new byte[binaryKey.Length - TIMESTAMP_SIZE];
//            System.arraycopy(binaryKey, 0, bytes, 0, bytes.Length);
//            K key = deserializer.Deserialize(topic, bytes);
//            Window window = extractWindow(binaryKey, windowSize);
//            return new Windowed<K>(key, window);
//        }

//        private static Window extractWindow(byte[] binaryKey,
//                                            long windowSize)
//        {
//            ByteBuffer buffer = ByteBuffer.Wrap(binaryKey);
//            long start = buffer.getLong(binaryKey.Length - TIMESTAMP_SIZE);
//            return timeWindowForSize(start, windowSize);
//        }

//        // for store serdes

//        public static Bytes toStoreKeyBinary(Bytes key,
//                                             long timestamp,
//                                             int seqnum)
//        {
//            byte[] serializedKey = key.Get();
//            return toStoreKeyBinary(serializedKey, timestamp, seqnum);
//        }

//        public static Bytes toStoreKeyBinary<K>(K key,
//                                                 long timestamp,
//                                                 int seqnum,
//                                                 StateSerdes<K, object> serdes)
//        {
//            byte[] serializedKey = serdes.rawKey(key);
//            return toStoreKeyBinary(serializedKey, timestamp, seqnum);
//        }

//        public static Bytes toStoreKeyBinary(Windowed<Bytes> timeKey,
//                                             int seqnum)
//        {
//            byte[] bytes = timeKey.key.Get();
//            return toStoreKeyBinary(bytes, timeKey.window.start(), seqnum);
//        }

//        public static Bytes toStoreKeyBinary<K>(Windowed<K> timeKey,
//                                                 int seqnum,
//                                                 StateSerdes<K, object> serdes)
//        {
//            byte[] serializedKey = serdes.rawKey(timeKey.key);
//            return toStoreKeyBinary(serializedKey, timeKey.window.start(), seqnum);
//        }

//        // package private for testing
//        static Bytes toStoreKeyBinary(byte[] serializedKey,
//                                      long timestamp,
//                                      int seqnum)
//        {
//            ByteBuffer buf = new ByteBuffer().Allocate(serializedKey.Length + TIMESTAMP_SIZE + SEQNUM_SIZE);
//            buf.Add(serializedKey);
//            buf.putLong(timestamp);
//            buf.putInt(seqnum);

//            return Bytes.Wrap(buf.array());
//        }

//        static byte[] extractStoreKeyBytes(byte[] binaryKey)
//        {
//            byte[] bytes = new byte[binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE];
//            System.arraycopy(binaryKey, 0, bytes, 0, bytes.Length);
//            return bytes;
//        }

//        static K extractStoreKey<K>(byte[] binaryKey,
//                                     StateSerdes<K, object> serdes)
//        {
//            byte[] bytes = new byte[binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE];
//            System.arraycopy(binaryKey, 0, bytes, 0, bytes.Length);
//            return serdes.keyFrom(bytes);
//        }

//        public static long extractStoreTimestamp(byte[] binaryKey)
//        {
//            return ByteBuffer.Wrap(binaryKey).getLong(binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE);
//        }

//        static int extractStoreSequence(byte[] binaryKey)
//        {
//            return ByteBuffer.Wrap(binaryKey).getInt(binaryKey.Length - SEQNUM_SIZE);
//        }

//        public static Windowed<K> fromStoreKey<K>(byte[] binaryKey,
//                                                   long windowSize,
//                                                   IDeserializer<K> deserializer,
//                                                   string topic)
//        {
//            K key = deserializer.Deserialize(topic, extractStoreKeyBytes(binaryKey));
//            Window window = extractStoreWindow(binaryKey, windowSize);
//            return new Windowed<>(key, window);
//        }

//        public static Windowed<K> fromStoreKey<K>(
//            Windowed<Bytes> windowedKey,
//            IDeserializer<K> deserializer,
//            string topic)
//        {
//            var key = deserializer.Deserialize(topic, windowedKey.key.Get());
//            return new Windowed<K>(key, windowedKey.window);
//        }

//        public static Windowed<Bytes> fromStoreBytesKey(byte[] binaryKey,
//                                                        long windowSize)
//        {
//            Bytes key = Bytes.Wrap(extractStoreKeyBytes(binaryKey));
//            Window window = extractStoreWindow(binaryKey, windowSize);
//            return new Windowed<Bytes>(key, window);
//        }

//        static Window extractStoreWindow(byte[] binaryKey,
//                                         long windowSize)
//        {
//            ByteBuffer buffer = ByteBuffer.Wrap(binaryKey);
//            long start = buffer.getLong(binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE);
//            return timeWindowForSize(start, windowSize);
//        }
//    }
//}