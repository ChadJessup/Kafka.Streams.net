///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.Topologies;
//using Xunit;

//namespace Kafka.Streams.KStream.Internals {


















































//public class KTableImplTest {
//    private Consumed<string, string> stringConsumed = Consumed.with(Serdes.String(), Serdes.String());
//    private Consumed<string, string> consumed = Consumed.with(Serdes.String(), Serdes.String());
//    private Produced<string, string> produced = Produced.with(Serdes.String(), Serdes.String());
//    private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());
//    private ConsumerRecordFactory<string, string> recordFactory =
//        new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L);
//    private ISerde<string> mySerde = new Serdes.StringSerde();

//    private IKTable<string, string> table;

    
//    public void setUp() {
//        table = new StreamsBuilder().Table("test");
//    }

//    [Fact]
//    public void testKTable() {
//        StreamsBuilder builder = new StreamsBuilder();

//        string topic1 = "topic1";
//        string topic2 = "topic2";

//        IKTable<string, string> table1 = builder.Table(topic1, consumed);

//        MockProcessorSupplier<string, object> supplier = new MockProcessorSupplier<>();
//        table1.toStream().process(supplier);

//        IKTable<string, int> table2 = table1.mapValues();
//        table2.toStream().process(supplier);

//        IKTable<string, int> table3 = table2.filter((key, value) => (value % 2) == 0);
//        table3.toStream().process(supplier);
//        table1.toStream().to(topic2, produced);

//        IKTable<string, string> table4 = builder.Table(topic2, consumed);
//        table4.toStream().process(supplier);

//        try {

//            driver.pipeInput(recordFactory.create(topic1, "A", "01", 5L));
//            driver.pipeInput(recordFactory.create(topic1, "B", "02", 100L));
//            driver.pipeInput(recordFactory.create(topic1, "C", "03", 0L));
//            driver.pipeInput(recordFactory.create(topic1, "D", "04", 0L));
//            driver.pipeInput(recordFactory.create(topic1, "A", "05", 10L));
//            driver.pipeInput(recordFactory.create(topic1, "A", "06", 8L));
//        }

//        List<MockProcessor<string, object>> processors = supplier.capturedProcessors(4);
//       Assert.EqualasList(new KeyValueTimestamp<>("A", "01", 5),
//                new KeyValueTimestamp<>("B", "02", 100),
//                new KeyValueTimestamp<>("C", "03", 0),
//                new KeyValueTimestamp<>("D", "04", 0),
//                new KeyValueTimestamp<>("A", "05", 10),
//                new KeyValueTimestamp<>("A", "06", 8)), processors.get(0).processed);
//       Assert.EqualasList(new KeyValueTimestamp<>("A", 1, 5),
//                new KeyValueTimestamp<>("B", 2, 100),
//                new KeyValueTimestamp<>("C", 3, 0),
//                new KeyValueTimestamp<>("D", 4, 0),
//                new KeyValueTimestamp<>("A", 5, 10),
//                new KeyValueTimestamp<>("A", 6, 8)), processors.get(1).processed);
//       Assert.EqualasList(new KeyValueTimestamp<>("A", null, 5),
//                new KeyValueTimestamp<>("B", 2, 100),
//                new KeyValueTimestamp<>("C", null, 0),
//                new KeyValueTimestamp<>("D", 4, 0),
//                new KeyValueTimestamp<>("A", null, 10),
//                new KeyValueTimestamp<>("A", 6, 8)), processors.get(2).processed);
//       Assert.EqualasList(new KeyValueTimestamp<>("A", "01", 5),
//                new KeyValueTimestamp<>("B", "02", 100),
//                new KeyValueTimestamp<>("C", "03", 0),
//                new KeyValueTimestamp<>("D", "04", 0),
//                new KeyValueTimestamp<>("A", "05", 10),
//                new KeyValueTimestamp<>("A", "06", 8)), processors.get(3).processed);
//    }

//    [Fact]
//    public void shouldPreserveSerdesForOperators() {
//        StreamsBuilder builder = new StreamsBuilder();
//        IKTable<string, string> table1 = builder.Table("topic-2", stringConsumed);
//        ConsumedInternal<string, string> consumedInternal = new ConsumedInternal<>(stringConsumed);

//        IKeyValueMapper<string, string, string> selector = (key, value) => key;
//        IValueMapper<string, string> mapper = value => value;
//        ValueJoiner<string, string, string> joiner = (value1, value2) => value1;
//        ValueTransformerWithKeySupplier<string, string, string> valueTransformerWithKeySupplier =
//            () => new ValueTransformerWithKey<string, string, string>() {
                
//                public void init(IProcessorContext context) {}

                
//                public string transform(string key, string value) {
//                    return value;
//                }

                
//                public void close() {}
//            };

//       Assert.Equal(
//            ((AbstractStream) table1.filter((key, value) => false)).keySerde(),
//            consumedInternal.keySerde());
//       Assert.Equal(
//            ((AbstractStream) table1.filter((key, value) => false)).valueSerde(),
//            consumedInternal.valueSerde());
//       Assert.Equal(
//            ((AbstractStream) table1.filter((key, value) => false, Materialized.with(mySerde, mySerde))).keySerde(),
//            mySerde);
//       Assert.Equal(
//            ((AbstractStream) table1.filter((key, value) => false, Materialized.with(mySerde, mySerde))).valueSerde(),
//            mySerde);

//       Assert.Equal(
//            ((AbstractStream) table1.filterNot((key, value) => false)).keySerde(),
//            consumedInternal.keySerde());
//       Assert.Equal(
//            ((AbstractStream) table1.filterNot((key, value) => false)).valueSerde(),
//            consumedInternal.valueSerde());
//       Assert.Equal(
//            ((AbstractStream) table1.filterNot((key, value) => false, Materialized.with(mySerde, mySerde))).keySerde(),
//            mySerde);
//       Assert.Equal(
//            ((AbstractStream) table1.filterNot((key, value) => false, Materialized.with(mySerde, mySerde))).valueSerde(),
//            mySerde);

//       Assert.Equal(
//            ((AbstractStream) table1.mapValues(mapper)).keySerde(),
//            consumedInternal.keySerde());
//       Assert.Null(((AbstractStream) table1.mapValues(mapper)).valueSerde());
//       Assert.Equal(
//            ((AbstractStream) table1.mapValues(mapper, Materialized.with(mySerde, mySerde))).keySerde(),
//            mySerde);
//       Assert.Equal(
//            ((AbstractStream) table1.mapValues(mapper, Materialized.with(mySerde, mySerde))).valueSerde(),
//            mySerde);

//       Assert.Equal(
//            ((AbstractStream) table1.toStream()).keySerde(),
//            consumedInternal.keySerde());
//       Assert.Equal(
//            ((AbstractStream) table1.toStream()).valueSerde(),
//            consumedInternal.valueSerde());
//       Assert.Null(((AbstractStream) table1.toStream(selector)).keySerde());
//       Assert.Equal(
//            ((AbstractStream) table1.toStream(selector)).valueSerde(),
//            consumedInternal.valueSerde());

//       Assert.Equal(
//            ((AbstractStream) table1.transformValues(valueTransformerWithKeySupplier)).keySerde(),
//            consumedInternal.keySerde());
//       Assert.Null(((AbstractStream) table1.transformValues(valueTransformerWithKeySupplier)).valueSerde());
//       Assert.Equal(
//            ((AbstractStream) table1.transformValues(valueTransformerWithKeySupplier, Materialized.with(mySerde, mySerde))).keySerde(),
//            mySerde);
//       Assert.Equal(((AbstractStream) table1.transformValues(valueTransformerWithKeySupplier, Materialized.with(mySerde, mySerde))).valueSerde(),
//            mySerde);

//       Assert.Null(((AbstractStream) table1.groupBy(KeyValue::new)).keySerde());
//       Assert.Null(((AbstractStream) table1.groupBy(KeyValue::new)).valueSerde());
//       Assert.Equal(
//            ((AbstractStream) table1.groupBy(KeyValue::new, Grouped.with(mySerde, mySerde))).keySerde(),
//            mySerde);
//       Assert.Equal(
//            ((AbstractStream) table1.groupBy(KeyValue::new, Grouped.with(mySerde, mySerde))).valueSerde(),
//            mySerde);

//       Assert.Equal(
//            ((AbstractStream) table1.join(table1, joiner)).keySerde(),
//            consumedInternal.keySerde());
//       Assert.Null(((AbstractStream) table1.join(table1, joiner)).valueSerde());
//       Assert.Equal(
//            ((AbstractStream) table1.join(table1, joiner, Materialized.with(mySerde, mySerde))).keySerde(),
//            mySerde);
//       Assert.Equal(
//            ((AbstractStream) table1.join(table1, joiner, Materialized.with(mySerde, mySerde))).valueSerde(),
//            mySerde);

//       Assert.Equal(
//            ((AbstractStream) table1.leftJoin(table1, joiner)).keySerde(),
//            consumedInternal.keySerde());
//       Assert.Null(((AbstractStream) table1.leftJoin(table1, joiner)).valueSerde());
//       Assert.Equal(
//            ((AbstractStream) table1.leftJoin(table1, joiner, Materialized.with(mySerde, mySerde))).keySerde(),
//            mySerde);
//       Assert.Equal(
//            ((AbstractStream) table1.leftJoin(table1, joiner, Materialized.with(mySerde, mySerde))).valueSerde(),
//            mySerde);

//       Assert.Equal(
//            ((AbstractStream) table1.outerJoin(table1, joiner)).keySerde(),
//            consumedInternal.keySerde());
//       Assert.Null(((AbstractStream) table1.outerJoin(table1, joiner)).valueSerde());
//       Assert.Equal(
//            ((AbstractStream) table1.outerJoin(table1, joiner, Materialized.with(mySerde, mySerde))).keySerde(),
//            mySerde);
//       Assert.Equal(
//            ((AbstractStream) table1.outerJoin(table1, joiner, Materialized.with(mySerde, mySerde))).valueSerde(),
//            mySerde);
//    }

//    [Fact]
//    public void testStateStoreLazyEval() {
//        StreamsBuilder builder = new StreamsBuilder();
//        string topic1 = "topic1";
//        string topic2 = "topic2";

//        IKTable<string, string, string> table1 =
//            (IKTable<string, string, string>) builder.Table(topic1, consumed);
//        builder.Table(topic2, consumed);

//        IKTable<string, string, int> table1Mapped =
//            (IKTable<string, string, int>) table1.mapValues();
//        table1Mapped.filter((key, value) => (value % 2) == 0);

//        try {

//           Assert.Equal(0, driver.getAllStateStores().size());
//        }
//    }

//    [Fact]
//    public void testStateStore() {
//        StreamsBuilder builder = new StreamsBuilder();
//        string topic1 = "topic1";
//        string topic2 = "topic2";

//        IKTable<string, string, string> table1 =
//            (IKTable<string, string, string>) builder.Table(topic1, consumed);
//        IKTable<string, string, string> table2 =
//            (IKTable<string, string, string>) builder.Table(topic2, consumed);

//        IKTable<string, string, int> table1Mapped =
//            (IKTable<string, string, int>) table1.mapValues();
//        IKTable<string, int, int> table1MappedFiltered =
//            (IKTable<string, int, int>) table1Mapped.filter((key, value) => (value % 2) == 0);
//        table2.join(table1MappedFiltered, (v1, v2) => v1 + v2);

//        try {

//           Assert.Equal(2, driver.getAllStateStores().size());
//        }
//    }

//    private void assertTopologyContainsProcessor(Topology topology, string processorName) {
//        for (TopologyDescription.Subtopology subtopology: topology.describe().subtopologies()) {
//            for (TopologyDescription.Node node: subtopology.nodes()) {
//                if (node.name().equals(processorName)) {
//                    return;
//                }
//            }
//        }
//        throw new AssertionError("No processor named '" + processorName + "'"
//            + "found in the provided Topology:\n" + topology.describe());
//    }

//    [Fact]
//    public void shouldCreateSourceAndSinkNodesForRepartitioningTopic()// throws Exception {

//        StreamsBuilder builder = new StreamsBuilder();
//        string topic1 = "topic1";
//        string storeName1 = "storeName1";

//        IKTable<string, string, string> table1 =
//            (IKTable<string, string, string>) builder.Table(
//                topic1,
//                consumed,
//                Materialize.As<string, string, IKeyValueStore<Bytes, byte[]>(storeName1)
//                    .withKeySerde(Serdes.String())
//                    .withValueSerde(Serdes.String())
//            );

//        table1.groupBy(MockMapper.noOpKeyValueMapper())
//            .aggregate(
//                MockInitializer.STRING_INIT,
//                MockAggregator.TOSTRING_ADDER,
//                MockAggregator.TOSTRING_REMOVER,
//                Materialized.As("mock-result1"));

//        table1.groupBy(MockMapper.noOpKeyValueMapper())
//            .reduce(
//                MockReducer.STRING_ADDER,
//                MockReducer.STRING_REMOVER,
//                Materialized.As("mock-result2"));

//        Topology topology = builder.Build();
//        try {


//           Assert.Equal(3, driver.getAllStateStores().size());

//           .AssertTopologyContainsProcessor(topology, "KSTREAM-SINK-0000000003");
//           .AssertTopologyContainsProcessor(topology, "KSTREAM-SOURCE-0000000004");
//           .AssertTopologyContainsProcessor(topology, "KSTREAM-SINK-0000000007");
//           .AssertTopologyContainsProcessor(topology, "KSTREAM-SOURCE-0000000008");

//            Field valSerializerField = ((SinkNode) driver.getProcessor("KSTREAM-SINK-0000000003"))
//                .getClass()
//                .getDeclaredField("valSerializer");
//            Field valDeserializerField = ((SourceNode) driver.getProcessor("KSTREAM-SOURCE-0000000004"))
//                .getClass()
//                .getDeclaredField("valDeserializer");
//            valSerializerField.setAccessible(true);
//            valDeserializerField.setAccessible(true);

//           Assert.NotNull(((ChangedSerializer) valSerializerField.get(driver.getProcessor("KSTREAM-SINK-0000000003"))).inner());
//           Assert.NotNull(((ChangedDeserializer) valDeserializerField.get(driver.getProcessor("KSTREAM-SOURCE-0000000004"))).inner());
//           Assert.NotNull(((ChangedSerializer) valSerializerField.get(driver.getProcessor("KSTREAM-SINK-0000000007"))).inner());
//           Assert.NotNull(((ChangedDeserializer) valDeserializerField.get(driver.getProcessor("KSTREAM-SOURCE-0000000008"))).inner());
//        }
//    }

//    [Fact](typeof(expected = NullPointerException))
//    public void shouldNotAllowNullSelectorOnToStream() {
//        table.toStream((IKeyValueMapper) null);
//    }

//    [Fact](typeof(expected = NullPointerException))
//    public void shouldNotAllowNullPredicateOnFilter() {
//        table.filter(null);
//    }

//    [Fact](typeof(expected = NullPointerException))
//    public void shouldNotAllowNullPredicateOnFilterNot() {
//        table.filterNot(null);
//    }

//    [Fact](typeof(expected = NullPointerException))
//    public void shouldNotAllowNullMapperOnMapValues() {
//        table.mapValues((ValueMapper) null);
//    }

//    [Fact](typeof(expected = NullPointerException))
//    public void shouldNotAllowNullMapperOnMapValueWithKey() {
//        table.mapValues((ValueMapperWithKey) null);
//    }

//    [Fact](typeof(expected = NullPointerException))
//    public void shouldNotAllowNullSelectorOnGroupBy() {
//        table.groupBy(null);
//    }

//    [Fact](typeof(expected = NullPointerException))
//    public void shouldNotAllowNullOtherTableOnJoin() {
//        table.join(null, MockValueJoiner.TOSTRING_JOINER);
//    }

//    [Fact]
//    public void shouldAllowNullStoreInJoin() {
//        table.join(table, MockValueJoiner.TOSTRING_JOINER);
//    }

//    [Fact](typeof(expected = NullPointerException))
//    public void shouldNotAllowNullJoinerJoin() {
//        table.join(table, null);
//    }

//    [Fact](typeof(expected = NullPointerException))
//    public void shouldNotAllowNullOtherTableOnOuterJoin() {
//        table.outerJoin(null, MockValueJoiner.TOSTRING_JOINER);
//    }

//    [Fact](typeof(expected = NullPointerException))
//    public void shouldNotAllowNullJoinerOnOuterJoin() {
//        table.outerJoin(table, null);
//    }

//    [Fact](typeof(expected = NullPointerException))
//    public void shouldNotAllowNullJoinerOnLeftJoin() {
//        table.leftJoin(table, null);
//    }

//    [Fact](typeof(expected = NullPointerException))
//    public void shouldNotAllowNullOtherTableOnLeftJoin() {
//        table.leftJoin(null, MockValueJoiner.TOSTRING_JOINER);
//    }

//    [Fact](typeof(expected = NullPointerException))
//    public void shouldThrowNullPointerOnFilterWhenMaterializedIsNull() {
//        table.filter((key, value) => false, (Materialized) null);
//    }

//    [Fact](typeof(expected = NullPointerException))
//    public void shouldThrowNullPointerOnFilterNotWhenMaterializedIsNull() {
//        table.filterNot((key, value) => false, (Materialized) null);
//    }

//    [Fact](typeof(expected = NullPointerException))
//    public void shouldThrowNullPointerOnJoinWhenMaterializedIsNull() {
//        table.join(table, MockValueJoiner.TOSTRING_JOINER, (Materialized) null);
//    }

//    [Fact](typeof(expected = NullPointerException))
//    public void shouldThrowNullPointerOnLeftJoinWhenMaterializedIsNull() {
//        table.leftJoin(table, MockValueJoiner.TOSTRING_JOINER, (Materialized) null);
//    }

//    [Fact](typeof(expected = NullPointerException))
//    public void shouldThrowNullPointerOnOuterJoinWhenMaterializedIsNull() {
//        table.outerJoin(table, MockValueJoiner.TOSTRING_JOINER, (Materialized) null);
//    }

//    [Fact](typeof(expected = NullPointerException))
//    public void shouldThrowNullPointerOnTransformValuesWithKeyWhenTransformerSupplierIsNull() {
//        table.transformValues((ValueTransformerWithKeySupplier) null);
//    }

    
//    [Fact](typeof(expected = NullPointerException))
//    public void shouldThrowNullPointerOnTransformValuesWithKeyWhenMaterializedIsNull() {
//        ValueTransformerWithKeySupplier<string, string, ?> valueTransformerSupplier =
//            mock(typeof(ValueTransformerWithKeySupplier));
//        table.transformValues(valueTransformerSupplier, (Materialized) null);
//    }

    
//    [Fact](typeof(expected = NullPointerException))
//    public void shouldThrowNullPointerOnTransformValuesWithKeyWhenStoreNamesNull() {
//        ValueTransformerWithKeySupplier<string, string, ?> valueTransformerSupplier =
//            mock(typeof(ValueTransformerWithKeySupplier));
//        table.transformValues(valueTransformerSupplier, (string[]) null);
//    }
//}
