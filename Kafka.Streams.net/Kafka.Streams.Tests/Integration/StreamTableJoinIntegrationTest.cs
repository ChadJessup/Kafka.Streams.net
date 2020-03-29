/*






 *

 *





 */























/**
 * Tests all available joins of Kafka Streams DSL.
 */


public class StreamTableJoinIntegrationTest : AbstractJoinIntegrationTest {
    private KStream<long, string> leftStream;
    private KTable<long, string> rightTable;

    public StreamTableJoinIntegrationTest(bool cacheEnabled) {
        super(cacheEnabled);
    }

    
    public void PrepareTopology() {// throws InterruptedException
        base.prepareEnvironment();

        appID = "stream-table-join-integration-test";

        builder = new StreamsBuilder();
        rightTable = builder.table(INPUT_TOPIC_RIGHT);
        leftStream = builder.stream(INPUT_TOPIC_LEFT);
    }

    [Xunit.Fact]
    public void TestShouldAutoShutdownOnIncompleteMetadata() {// throws InterruptedException
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-incomplete");

        KStream<long, string> notExistStream = builder.stream(INPUT_TOPIC_LEFT + "-not-existed");

        KTable<long, string> aggregatedTable = notExistStream.leftJoin(rightTable, valueJoiner)
                .groupBy((key, value) => key)
                .reduce((value1, value2) => value1 + value2);

        // Write the (continuously updating) results to the output topic.
        aggregatedTable.toStream().to(OUTPUT_TOPIC);

        KafkaStreamsWrapper streams = new KafkaStreamsWrapper(builder.build(), STREAMS_CONFIG);
        IntegrationTestUtils.StateListenerStub listener = new IntegrationTestUtils.StateListenerStub();
        streams.setStreamThreadStateListener(listener);
        streams.start();

        TestUtils.waitForCondition(listener::revokedToPendingShutdownSeen, "Did not seen thread state transited to PENDING_SHUTDOWN");

        streams.close();
        Assert.True(listener.createdToRevokedSeen());
        Assert.True(listener.revokedToPendingShutdownSeen());
    }

    [Xunit.Fact]
    public void TestInner() {// throws Exception
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner");

        List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
            null,
            null,
            null,
            null,
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a", 5L)),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d", 15L))
        );

        leftStream.join(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    [Xunit.Fact]
    public void TestLeft() {// throws Exception
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left");

        List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
            null,
            null,
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-null", 3L)),
            null,
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a", 5L)),
            null,
            null,
            null,
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-null", 9L)),
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d", 15L))
        );

        leftStream.leftJoin(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }
}
