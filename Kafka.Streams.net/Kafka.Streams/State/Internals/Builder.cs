

//            public IStoreBuilder<InMemoryTimeOrderedKeyValueBuffer<K, V>> WithCachingDisabled()
//            {
//                return this;
//            }


//            public IStoreBuilder<InMemoryTimeOrderedKeyValueBuffer<K, V>> withLoggingEnabled(Dictionary<string, string> config)
//            {
//                throw new InvalidOperationException();
//            }


//            public IStoreBuilder<InMemoryTimeOrderedKeyValueBuffer<K, V>> WithLoggingDisabled()
//            {
//                loggingEnabled = false;
//                return this;
//            }


//            public InMemoryTimeOrderedKeyValueBuffer<K, V> build()
//            {
//                return new InMemoryTimeOrderedKeyValueBuffer<>(storeName, loggingEnabled, keySerde, valSerde);
//            }


//            public Dictionary<string, string> logConfig()
//            {
//                return Collections.emptyMap();
//            }


//            public bool loggingEnabled { get; }
//            public string Name => storeName;
//        }
//    }
//}
