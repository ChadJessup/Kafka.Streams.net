//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */

















//    public class QueryableStoreProviderTest
//    {

//        private readonly string KeyValueStore = "key-value";
//        private readonly string windowStore = "window-store";
//        private QueryableStoreProvider storeProvider;
//        private HashDictionary<string, IStateStore> globalStateStores;


//        public void Before()
//        {
//            StateStoreProviderStub theStoreProvider = new StateStoreProviderStub(false);
//            theStoreProvider.addStore(KeyValueStore, new NoOpReadOnlyStore<>());
//            theStoreProvider.addStore(windowStore, new NoOpWindowStore());
//            globalStateStores = new HashMap<>();
//            storeProvider =
//                new QueryableStoreProvider(
//                        Collections.< StateStoreProvider > Collections.singletonList(theStoreProvider), new GlobalStateStoreProvider(globalStateStores));
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowExceptionIfKVStoreDoesntExist()
//        {
//            storeProvider.getStore("not-a-store", QueryableStoreTypes.KeyValueStore);
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowExceptionIfWindowStoreDoesntExist()
//        {
//            storeProvider.getStore("not-a-store", QueryableStoreTypes.windowStore());
//        }

//        [Fact]
//        public void ShouldReturnKVStoreWhenItExists()
//        {
//            Assert.NotNull(storeProvider.getStore(KeyValueStore, QueryableStoreTypes.KeyValueStore));
//        }

//        [Fact]
//        public void ShouldReturnWindowStoreWhenItExists()
//        {
//            Assert.NotNull(storeProvider.getStore(windowStore, QueryableStoreTypes.windowStore()));
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowExceptionWhenLookingForWindowStoreWithDifferentType()
//        {
//            storeProvider.getStore(windowStore, QueryableStoreTypes.KeyValueStore);
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowExceptionWhenLookingForKVStoreWithDifferentType()
//        {
//            storeProvider.getStore(KeyValueStore, QueryableStoreTypes.windowStore());
//        }

//        [Fact]
//        public void ShouldFindGlobalStores()
//        {
//            globalStateStores.Put("global", new NoOpReadOnlyStore<>());
//            Assert.NotNull(storeProvider.getStore("global", QueryableStoreTypes.KeyValueStore));
//        }


//    }
//}
///*






//*

//*





//*/

















