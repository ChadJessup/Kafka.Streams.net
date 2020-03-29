/*






 *

 *





 */



































public class KStreamTransformIntegrationTest {

    private StreamsBuilder builder;
    private string topic = "stream";
    private string stateStoreName = "myTransformState";
    private List<KeyValuePair<int, int>> results = new ArrayList<>();
    private ForeachAction<int, int> action = (key, value) => results.add(KeyValuePair.Create(key, value));
    private KStream<int, int> stream;

    
    public void before() {
        builder = new StreamsBuilder();
        StoreBuilder<KeyValueStore<int, int>> keyValueStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
                                            Serdes.Int(),
                                            Serdes.Int());
        builder.addStateStore(keyValueStoreBuilder);
        stream = builder.stream(topic, Consumed.with(Serdes.Int(), Serdes.Int()));
    }

    private void verifyResult(List<KeyValuePair<int, int>> expected) {
        ConsumerRecordFactory<int, int> recordFactory =
            new ConsumerRecordFactory<>(new IntegerSerializer(), new IntegerSerializer());
        Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Int(), Serdes.Int());
        try { 
 (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props));
            driver.pipeInput(recordFactory.create(topic, Array.asList(new KeyValuePair<>(1, 1),
                                                                       new KeyValuePair<>(2, 2),
                                                                       new KeyValuePair<>(3, 3),
                                                                       new KeyValuePair<>(2, 1),
                                                                       new KeyValuePair<>(2, 3),
                                                                       new KeyValuePair<>(1, 3))));
        }
        Assert.Equal(results, (expected));
    }

    [Xunit.Fact]
    public void shouldTransform() {
        stream
            .transform(() => new Transformer<int, int, KeyValuePair<int, int>>() {
                private KeyValueStore<int, int> state;

                
                
                public void init(ProcessorContext context) {
                    state = (KeyValueStore<int, int>) context.getStateStore(stateStoreName);
                }

                
                public KeyValuePair<int, int> transform(int key, int value) {
                    state.putIfAbsent(key, 0);
                    int storedValue = state.get(key);
                    KeyValuePair<int, int> result = new KeyValuePair<>(key + 1, value + storedValue++);
                    state.put(key, storedValue);
                    return result;
                }

                
                public void close() {
                }
            }, "myTransformState")
            .foreach(action);

        List<KeyValuePair<int, int>> expected = Array.asList(
            KeyValuePair.Create(2, 1),
            KeyValuePair.Create(3, 2),
            KeyValuePair.Create(4, 3),
            KeyValuePair.Create(3, 2),
            KeyValuePair.Create(3, 5),
            KeyValuePair.Create(2, 4));
        verifyResult(expected);
    }

    [Xunit.Fact]
    public void shouldFlatTransform() {
        stream
            .flatTransform(() => new Transformer<int, int, Iterable<KeyValuePair<int, int>>>() {
                private KeyValueStore<int, int> state;

                
                
                public void init(ProcessorContext context) {
                    state = (KeyValueStore<int, int>) context.getStateStore(stateStoreName);
                }

                
                public Iterable<KeyValuePair<int, int>> transform(int key, int value) {
                    List<KeyValuePair<int, int>> result = new ArrayList<>();
                    state.putIfAbsent(key, 0);
                    int storedValue = state.get(key);
                    for (int i = 0; i < 3; i++) {
                        result.add(new KeyValuePair<>(key + i, value + storedValue++));
                    }
                    state.put(key, storedValue);
                    return result;
                }

                
                public void close() {
                }
            }, "myTransformState")
            .foreach(action);

        List<KeyValuePair<int, int>> expected = Array.asList(
            KeyValuePair.Create(1, 1),
            KeyValuePair.Create(2, 2),
            KeyValuePair.Create(3, 3),
            KeyValuePair.Create(2, 2),
            KeyValuePair.Create(3, 3),
            KeyValuePair.Create(4, 4),
            KeyValuePair.Create(3, 3),
            KeyValuePair.Create(4, 4),
            KeyValuePair.Create(5, 5),
            KeyValuePair.Create(2, 4),
            KeyValuePair.Create(3, 5),
            KeyValuePair.Create(4, 6),
            KeyValuePair.Create(2, 9),
            KeyValuePair.Create(3, 10),
            KeyValuePair.Create(4, 11),
            KeyValuePair.Create(1, 6),
            KeyValuePair.Create(2, 7),
            KeyValuePair.Create(3, 8));
        verifyResult(expected);
    }

    [Xunit.Fact]
    public void shouldTransformValuesWithValueTransformerWithKey() {
        stream
            .transformValues(() => new ValueTransformerWithKey<int, int, int>() {
                private KeyValueStore<int, int> state;

                
                public void init(ProcessorContext context) {
                    state = (KeyValueStore<int, int>) context.getStateStore("myTransformState");
                }

                
                public int transform(int key, int value) {
                    state.putIfAbsent(key, 0);
                    int storedValue = state.get(key);
                    int result = value + storedValue++;
                    state.put(key, storedValue);
                    return result;
                }

                
                public void close() {
                }
            }, "myTransformState")
            .foreach(action);

        List<KeyValuePair<int, int>> expected = Array.asList(
            KeyValuePair.Create(1, 1),
            KeyValuePair.Create(2, 2),
            KeyValuePair.Create(3, 3),
            KeyValuePair.Create(2, 2),
            KeyValuePair.Create(2, 5),
            KeyValuePair.Create(1, 4));
        verifyResult(expected);
    }

    [Xunit.Fact]
    public void shouldTransformValuesWithValueTransformerWithoutKey() {
        stream
            .transformValues(() => new ValueTransformer<int, int>() {
                private KeyValueStore<int, int> state;

                
                public void init(ProcessorContext context) {
                    state = (KeyValueStore<int, int>) context.getStateStore("myTransformState");
                }

                
                public int transform(int value) {
                    state.putIfAbsent(value, 0);
                    int counter = state.get(value);
                    state.put(value, ++counter);
                    return counter;
                }

                
                public void close() {
                }
            }, "myTransformState")
            .foreach(action);

        List<KeyValuePair<int, int>> expected = Array.asList(
            KeyValuePair.Create(1, 1),
            KeyValuePair.Create(2, 1),
            KeyValuePair.Create(3, 1),
            KeyValuePair.Create(2, 2),
            KeyValuePair.Create(2, 2),
            KeyValuePair.Create(1, 3));
        verifyResult(expected);
    }

    [Xunit.Fact]
    public void shouldFlatTransformValuesWithKey() {
        stream
            .flatTransformValues(() => new ValueTransformerWithKey<int, int, Iterable<int>>() {
                private KeyValueStore<int, int> state;

                
                public void init(ProcessorContext context) {
                    state = (KeyValueStore<int, int>) context.getStateStore("myTransformState");
                }

                
                public Iterable<int> transform(int key, int value) {
                    List<int> result = new ArrayList<>();
                    state.putIfAbsent(key, 0);
                    int storedValue = state.get(key);
                    for (int i = 0; i < 3; i++) {
                        result.add(value + storedValue++);
                    }
                    state.put(key, storedValue);
                    return result;
                }

                
                public void close() {
                }
            }, "myTransformState")
            .foreach(action);

        List<KeyValuePair<int, int>> expected = Array.asList(
            KeyValuePair.Create(1, 1),
            KeyValuePair.Create(1, 2),
            KeyValuePair.Create(1, 3),
            KeyValuePair.Create(2, 2),
            KeyValuePair.Create(2, 3),
            KeyValuePair.Create(2, 4),
            KeyValuePair.Create(3, 3),
            KeyValuePair.Create(3, 4),
            KeyValuePair.Create(3, 5),
            KeyValuePair.Create(2, 4),
            KeyValuePair.Create(2, 5),
            KeyValuePair.Create(2, 6),
            KeyValuePair.Create(2, 9),
            KeyValuePair.Create(2, 10),
            KeyValuePair.Create(2, 11),
            KeyValuePair.Create(1, 6),
            KeyValuePair.Create(1, 7),
            KeyValuePair.Create(1, 8));
        verifyResult(expected);
    }

    [Xunit.Fact]
    public void shouldFlatTransformValuesWithValueTransformerWithoutKey() {
        stream
            .flatTransformValues(() => new ValueTransformer<int, Iterable<int>>() {
                private KeyValueStore<int, int> state;

                
                public void init(ProcessorContext context) {
                    state = (KeyValueStore<int, int>) context.getStateStore("myTransformState");
                }

                
                public Iterable<int> transform(int value) {
                    List<int> result = new ArrayList<>();
                    state.putIfAbsent(value, 0);
                    int counter = state.get(value);
                    for (int i = 0; i < 3; i++) {
                        result.add(++counter);
                    }
                    state.put(value, counter);
                    return result;
                }

                
                public void close() {
                }
            }, "myTransformState")
            .foreach(action);

        List<KeyValuePair<int, int>> expected = Array.asList(
            KeyValuePair.Create(1, 1),
            KeyValuePair.Create(1, 2),
            KeyValuePair.Create(1, 3),
            KeyValuePair.Create(2, 1),
            KeyValuePair.Create(2, 2),
            KeyValuePair.Create(2, 3),
            KeyValuePair.Create(3, 1),
            KeyValuePair.Create(3, 2),
            KeyValuePair.Create(3, 3),
            KeyValuePair.Create(2, 4),
            KeyValuePair.Create(2, 5),
            KeyValuePair.Create(2, 6),
            KeyValuePair.Create(2, 4),
            KeyValuePair.Create(2, 5),
            KeyValuePair.Create(2, 6),
            KeyValuePair.Create(1, 7),
            KeyValuePair.Create(1, 8),
            KeyValuePair.Create(1, 9));
        verifyResult(expected);
    }
}
