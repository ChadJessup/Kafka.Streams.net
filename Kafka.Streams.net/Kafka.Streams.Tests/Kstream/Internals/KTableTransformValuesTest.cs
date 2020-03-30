namespace Kafka.Streams.Tests.Kstream.Internals
{
}
//using Kafka.Streams.Configs;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.State.TimeStamped;
//using Kafka.Streams.Tests.Helpers;
//using System.Collections.Generic;
//using System.Linq;
//using Xunit;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableTransformValuesTest
//    {
//        private static string QUERYABLE_NAME = "queryable-store";
//        private static string INPUT_TOPIC = "inputTopic";
//        private static string STORE_NAME = "someStore";
//        private static string OTHER_STORE_NAME = "otherStore";

//        private static Consumed<string, string> CONSUMED = Consumed.with(Serdes.String(), Serdes.String());

//        private ConsumerRecordFactory<string, string> recordFactory =
//            new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L);

//        private var driver;
//        private MockProcessorSupplier<string, string> capture;
//        private StreamsBuilder builder;

//        private IKTable<string, string> parent;

//        private IInternalProcessorContext context;

//        private KTableValueGetterSupplier<string, string> parentGetterSupplier;

//        private IKTableValueGetter<string, string> parentGetter;

//        private ITimestampedKeyValueStore<string, string> stateStore;

//        private IValueTransformerWithKeySupplier<string, string, string> mockSupplier;

//        private IValueTransformerWithKey<string, string, string> transformer;


//        public void cleanup()
//        {
//            if (driver != null)
//            {
//                driver.close();
//                driver = null;
//            }
//        }


//        public void setUp()
//        {
//            capture = new MockProcessorSupplier<>();
//            builder = new StreamsBuilder();
//        }

//        [Fact]
//        public void shouldThrowOnGetIfSupplierReturnsNull()
//        {
//            KTableTransformValues<string, string, string> transformer =
//                new KTableTransformValues<>(parent, new NullSupplier(), QUERYABLE_NAME);

//            try
//            {
//                transformer.get();
//                Assert.False(true, "NPE expected");
//            }
//            catch (NullPointerException expected)
//            {
//                // expected
//            }
//        }

//        [Fact]
//        public void shouldThrowOnViewGetIfSupplierReturnsNull()
//        {
//            KTableValueGetterSupplier<string, string> view =
//                new KTableTransformValues<>(parent, new NullSupplier(), null).view();

//            try
//            {
//                view.get();
//                Assert.False(true, "NPE expected");
//            }
//            catch (NullPointerException expected)
//            {
//                // expected
//            }
//        }


//        [Fact]
//        public void shouldInitializeTransformerWithForwardDisabledProcessorContext()
//        {
//            SingletonNoOpValueTransformer<string, string> transformer = new SingletonNoOpValueTransformer<>();
//            KTableTransformValues<string, string, string> transformValues =
//                new KTableTransformValues<>(parent, transformer, null);
//            Processor<string, Change<string>> processor = transformValues.get();

//            processor.init(context);

//            Assert.Equal(transformer.context, typeof(ForwardingDisabledProcessorContext)));
//        }

//        [Fact]
//        public void shouldNotSendOldValuesByDefault()
//        {
//            KTableTransformValues<string, string, string> transformValues =
//                new KTableTransformValues<>(parent, new ExclamationValueTransformerSupplier(), null);

//            Processor<string, Change<string>> processor = transformValues.get();
//            processor.init(context);

//            context.forward("Key", new Change<>("Key=>newValue!", null));
//            expect.AstCall();
//            replay(context);

//            processor.process("Key", new Change<>("newValue", "oldValue"));

//            verify(context);
//        }

//        [Fact]
//        public void shouldSendOldValuesIfConfigured()
//        {
//            KTableTransformValues<string, string, string> transformValues =
//                new KTableTransformValues<>(parent, new ExclamationValueTransformerSupplier(), null);

//            transformValues.enableSendingOldValues();
//            Processor<string, Change<string>> processor = transformValues.get();
//            processor.init(context);

//            context.forward("Key", new Change<>("Key=>newValue!", "Key=>oldValue!"));
//            expect.AstCall();
//            replay(context);

//            processor.process("Key", new Change<>("newValue", "oldValue"));

//            verify(context);
//        }

//        [Fact]
//        public void shouldSetSendOldValuesOnParent()
//        {
//            parent.enableSendingOldValues();
//            expect.AstCall();
//            replay(parent);

//            new KTableTransformValues<>(parent, new SingletonNoOpValueTransformer<>(), QUERYABLE_NAME).enableSendingOldValues();

//            verify(parent);
//        }


//        [Fact]
//        public void shouldTransformOnGetIfNotMaterialized()
//        {
//            KTableTransformValues<string, string, string> transformValues =
//                new KTableTransformValues<>(parent, new ExclamationValueTransformerSupplier(), null);

//            expect(parent.valueGetterSupplier()).andReturn(parentGetterSupplier);
//            expect(parentGetterSupplier.get()).andReturn(parentGetter);
//            expect(parentGetter.get("Key")).andReturn(ValueAndTimestamp.make("Value", -1L));
//            replay(parent, parentGetterSupplier, parentGetter);

//            IKTableValueGetter<string, string> getter = transformValues.view().get();
//            getter.init(context);

//            string result = getter.get("Key").Value;

//            Assert.Equal(result, ("Key=>Value!"));
//        }

//        [Fact]
//        public void shouldGetFromStateStoreIfMaterialized()
//        {
//            KTableTransformValues<string, string, string> transformValues =
//                new KTableTransformValues<>(parent, new ExclamationValueTransformerSupplier(), QUERYABLE_NAME);

//            expect(context.getStateStore(QUERYABLE_NAME)).andReturn(stateStore);
//            expect(stateStore.get("Key")).andReturn(ValueAndTimestamp.make("something", 0L));
//            replay(context, stateStore);

//            KTableValueGetter<string, string> getter = transformValues.view().get();
//            getter.init(context);

//            string result = getter.get("Key").Value;

//            Assert.Equal(result, ("something"));
//        }

//        [Fact]
//        public void shouldGetStoreNamesFromParentIfNotMaterialized()
//        {
//            KTableTransformValues<string, string, string> transformValues =
//                new KTableTransformValues<>(parent, new ExclamationValueTransformerSupplier(), null);

//            expect(parent.valueGetterSupplier()).andReturn(parentGetterSupplier);
//            expect(parentGetterSupplier.storeNames()).andReturn(new string[] { "store1", "store2" });
//            replay(parent, parentGetterSupplier);

//            string[] storeNames = transformValues.view().storeNames();

//            Assert.Equal(storeNames, (new string[] { "store1", "store2" }));
//        }

//        [Fact]
//        public void shouldGetQueryableStoreNameIfMaterialized()
//        {
//            KTableTransformValues<string, string, string> transformValues =
//                new KTableTransformValues<>(parent, new ExclamationValueTransformerSupplier(), QUERYABLE_NAME);

//            string[] storeNames = transformValues.view().storeNames();

//            Assert.Equal(storeNames, (new string[] { QUERYABLE_NAME }));
//        }

//        [Fact]
//        public void shouldCloseTransformerOnProcessorClose()
//        {
//            KTableTransformValues<string, string, string> transformValues =
//                new KTableTransformValues<>(parent, mockSupplier, null);

//            expect(mockSupplier.get()).andReturn(transformer);
//            transformer.close();
//            expect.AstCall();
//            replay(mockSupplier, transformer);

//            Processor<string, Change<string>> processor = transformValues.get();
//            processor.close();

//            verify(transformer);
//        }

//        [Fact]
//        public void shouldCloseTransformerOnGetterClose()
//        {
//            KTableTransformValues<string, string, string> transformValues =
//                new KTableTransformValues<>(parent, mockSupplier, null);

//            expect(mockSupplier.get()).andReturn(transformer);
//            expect(parentGetterSupplier.get()).andReturn(parentGetter);
//            expect(parent.valueGetterSupplier()).andReturn(parentGetterSupplier);

//            transformer.close();
//            expect.AstCall();

//            replay(mockSupplier, transformer, parent, parentGetterSupplier);

//            KTableValueGetter<string, string> getter = transformValues.view().get();
//            getter.close();

//            verify(transformer);
//        }

//        [Fact]
//        public void shouldCloseParentGetterClose()
//        {
//            KTableTransformValues<string, string, string> transformValues =
//                new KTableTransformValues<>(parent, mockSupplier, null);

//            expect(parent.valueGetterSupplier()).andReturn(parentGetterSupplier);
//            expect(mockSupplier.get()).andReturn(transformer);
//            expect(parentGetterSupplier.get()).andReturn(parentGetter);

//            parentGetter.close();
//            expect.AstCall();

//            replay(mockSupplier, parent, parentGetterSupplier, parentGetter);

//            KTableValueGetter<string, string> getter = transformValues.view().get();
//            getter.close();

//            verify(parentGetter);
//        }

//        [Fact]
//        public void shouldTransformValuesWithKey()
//        {
//            builder
//                .addStateStore(storeBuilder(STORE_NAME))
//                .addStateStore(storeBuilder(OTHER_STORE_NAME))
//                .Table(INPUT_TOPIC, CONSUMED)
//                .transformValues(
//                    new ExclamationValueTransformerSupplier(STORE_NAME, OTHER_STORE_NAME),
//                    STORE_NAME, OTHER_STORE_NAME)
//                .toStream()
//                .process(capture);

//            driver = new TopologyTestDriver(builder.Build(), props());

//            driver.pipeInput(recordFactory.create(INPUT_TOPIC, "A", "a", 5L));
//            driver.pipeInput(recordFactory.create(INPUT_TOPIC, "B", "b", 10L));
//            driver.pipeInput(recordFactory.create(INPUT_TOPIC, "D", (string)null, 15L));


//            Assert.Equal(output(), asItems(new KeyValueTimestamp<>("A", "A=>a!", 5),
//                    new KeyValueTimestamp<>("B", "B=>b!", 10),
//                    new KeyValueTimestamp<>("D", "D=>null!", 15)
//            ));
//            Assert.Null("Store should not be materialized", driver.getKeyValueStore(QUERYABLE_NAME));
//        }

//        [Fact]
//        public void shouldTransformValuesWithKeyAndMaterialize()
//        {
//            builder
//                .addStateStore(storeBuilder(STORE_NAME))
//                .Table(INPUT_TOPIC, CONSUMED)
//                .transformValues(
//                    new ExclamationValueTransformerSupplier(STORE_NAME, QUERYABLE_NAME),
//                    Materialize.As < string, string, IKeyValueStore<Bytes, byte[]>(QUERYABLE_NAME)
//                        .withKeySerde(Serdes.String())
//                        .withValueSerde(Serdes.String()),
//                    STORE_NAME)
//                .toStream()
//                .process(capture);

//            driver = new TopologyTestDriver(builder.Build(), props());

//            driver.pipeInput(recordFactory.create(INPUT_TOPIC, "A", "a", 5L));
//            driver.pipeInput(recordFactory.create(INPUT_TOPIC, "B", "b", 10L));
//            driver.pipeInput(recordFactory.create(INPUT_TOPIC, "C", (string)null, 15L));

//            Assert.Equal(output(), asItems(new KeyValueTimestamp<>("A", "A=>a!", 5),
//                    new KeyValueTimestamp<>("B", "B=>b!", 10),
//                    new KeyValueTimestamp<>("C", "C=>null!", 15)));

//            {
//                IKeyValueStore<string, string> keyValueStore = driver.getKeyValueStore(QUERYABLE_NAME);
//                Assert.Equal(keyValueStore.get("A"), ("A=>a!"));
//                Assert.Equal(keyValueStore.get("B"), ("B=>b!"));
//                Assert.Equal(keyValueStore.get("C"), ("C=>null!"));
//            }
//            {
//                IKeyValueStore<string, ValueAndTimestamp<string>> keyValueStore = driver.getTimestampedKeyValueStore(QUERYABLE_NAME);
//                Assert.Equal(keyValueStore.get("A"), (ValueAndTimestamp.make("A=>a!", 5L)));
//                Assert.Equal(keyValueStore.get("B"), (ValueAndTimestamp.make("B=>b!", 10L)));
//                Assert.Equal(keyValueStore.get("C"), (ValueAndTimestamp.make("C=>null!", 15L)));
//            }
//        }

//        [Fact]
//        public void shouldCalculateCorrectOldValuesIfMaterializedEvenIfStateful()
//        {
//            builder
//                .Table(INPUT_TOPIC, CONSUMED)
//                .transformValues(
//                    new StatefulTransformerSupplier(),
//                    Materialize.As < string, int, IKeyValueStore<Bytes, byte[]>(QUERYABLE_NAME)
//                        .withKeySerde(Serdes.String())
//                        .withValueSerde(Serdes.Int()))
//                .groupBy(toForceSendingOfOldValues(), Grouped.with(Serdes.String(), Serdes.Int()))
//                .reduce(MockReducer.INTEGER_ADDER, MockReducer.INTEGER_SUBTRACTOR)
//                .mapValues(mapBackToStrings())
//                .toStream()
//                .process(capture);

//            driver = new TopologyTestDriver(builder.Build(), props());

//            driver.pipeInput(recordFactory.create(INPUT_TOPIC, "A", "ignored", 5L));
//            driver.pipeInput(recordFactory.create(INPUT_TOPIC, "A", "ignored", 15L));
//            driver.pipeInput(recordFactory.create(INPUT_TOPIC, "A", "ignored", 10L));

//            Assert.Equal(output(), asItems(new KeyValueTimestamp<>("A", "1", 5),
//                    new KeyValueTimestamp<>("A", "0", 15),
//                    new KeyValueTimestamp<>("A", "2", 15),
//                    new KeyValueTimestamp<>("A", "0", 15),
//                    new KeyValueTimestamp<>("A", "3", 15)));

//            IKeyValueStore<string, int> keyValueStore = driver.getKeyValueStore(QUERYABLE_NAME);
//            Assert.Equal(keyValueStore.get("A"), (3));
//        }

//        [Fact]
//        public void shouldCalculateCorrectOldValuesIfNotStatefulEvenIfNotMaterialized()
//        {
//            builder
//                .Table(INPUT_TOPIC, CONSUMED)
//                .transformValues(new StatelessTransformerSupplier())
//                .groupBy(toForceSendingOfOldValues(), Grouped.with(Serdes.String(), Serdes.Int()))
//                .reduce(MockReducer.INTEGER_ADDER, MockReducer.INTEGER_SUBTRACTOR)
//                .mapValues(mapBackToStrings())
//                .toStream()
//                .process(capture);

//            driver = new TopologyTestDriver(builder.Build(), props());

//            driver.pipeInput(recordFactory.create(INPUT_TOPIC, "A", "a", 5L));
//            driver.pipeInput(recordFactory.create(INPUT_TOPIC, "A", "aa", 15L));
//            driver.pipeInput(recordFactory.create(INPUT_TOPIC, "A", "aaa", 10));

//            Assert.Equal(output(), asItems(new KeyValueTimestamp<>("A", "1", 5),
//                     new KeyValueTimestamp<>("A", "0", 15),
//                     new KeyValueTimestamp<>("A", "2", 15),
//                     new KeyValueTimestamp<>("A", "0", 15),
//                     new KeyValueTimestamp<>("A", "3", 15)));
//        }

//        private List<KeyValueTimestamp<object, object>> output()
//        {
//            return capture.capturedProcessors(1).get(0).processed;
//        }

//        private IKeyValueMapper<string, int, KeyValuePair<string, int>> toForceSendingOfOldValues()
//        {
//            return null; // KeyValuePair::new;
//        }

//        private IValueMapper<int, string> mapBackToStrings()
//        {
//            return null; // object::ToString;
//        }

//        private IStoreBuilder<IKeyValueStore<long, long>> storeBuilder(string storeName)
//        {
//            return Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(storeName), Serdes.Long(), Serdes.Long());
//        }

//        public StreamsConfig props()
//        {
//            var props = new StreamsConfig();
//            props.Set(StreamsConfigPropertyNames.ApplicationId, "kstream-transform-values-test");
//            props.Set(StreamsConfigPropertyNames.BootstrapServers, "localhost:9091");
//            props.Set(StreamsConfigPropertyNames.STATE_DIR_CONFIG, TestUtils.GetTempDirectory());
//            props.Set(StreamsConfigPropertyNames.DefaultKeySerdeClass, Serdes.Int().GetType().FullName);
//            props.Set(StreamsConfigPropertyNames.DefaultValueSerdeClass, Serdes.Int().GetType().FullName);
//            return props;
//        }

//        private void throwIfStoresNotAvailable(IProcessorContext context,
//                                                      List<string> expectedStoredNames)
//        {
//            List<string> missing = new List<string>();

//            foreach (string storedName in expectedStoredNames)
//            {
//                if (context.getStateStore(storedName) == null)
//                {
//                    missing.Add(storedName);
//                }
//            }

//            if (missing.Any())
//            {
//                throw new AssertionError("State stores are not accessible: " + missing);
//            }
//        }

//        public class ExclamationValueTransformerSupplier : ValueTransformerWithKeySupplier<object, string, string>
//        {
//            private List<string> expectedStoredNames;

//            ExclamationValueTransformerSupplier(string... expectedStoreNames)
//            {
//                this.expectedStoredNames = new List<string> { expectedStoreNames };
//            }


//            public ExclamationValueTransformer get()
//            {
//                return new ExclamationValueTransformer(expectedStoredNames);
//            }
//        }

//        public class ExclamationValueTransformer : ValueTransformerWithKey<object, string, string>
//        {
//            private List<string> expectedStoredNames;

//            ExclamationValueTransformer(List<string> expectedStoredNames)
//            {
//                this.expectedStoredNames = expectedStoredNames;
//            }


//            public void init(IProcessorContext context)
//            {
//                throwIfStoresNotAvailable(context, expectedStoredNames);
//            }


//            public string transform(object readOnlyKey, string value)
//            {
//                return readOnlyKey.ToString() + "=>" + value + "!";
//            }


//            public void close() { }
//        }

//        private class NullSupplier : ValueTransformerWithKeySupplier<string, string, string>
//        {

//            public ValueTransformerWithKey<string, string, string> get()
//            {
//                return null;
//            }
//        }

//        private class StatefulTransformerSupplier : ValueTransformerWithKeySupplier<string, string, int>
//        {

//            public ValueTransformerWithKey<string, string, int> get()
//            {
//                return new StatefulTransformer();
//            }
//        }

//        private class StatefulTransformer : ValueTransformerWithKey<string, string, int>
//        {
//            private int counter;


//            public void init(IProcessorContext context) { }


//            public int transform(string readOnlyKey, string value)
//            {
//                return ++counter;
//            }


//            public void close() { }
//        }

//        private class StatelessTransformerSupplier : ValueTransformerWithKeySupplier<string, string, int>
//        {

//            public ValueTransformerWithKey<string, string, int> get()
//            {
//                return new StatelessTransformer();
//            }
//        }

//        private class StatelessTransformer : ValueTransformerWithKey<string, string, int>
//        {

//            public void init(IProcessorContext context) { }


//            public int transform(string readOnlyKey, string value)
//            {
//                return value == null ? null : value.Length();
//            }


//            public void close() { }
//        }
//    }
//}
