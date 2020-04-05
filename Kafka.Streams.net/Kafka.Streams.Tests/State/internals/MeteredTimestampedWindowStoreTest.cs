//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */

























//    public class MeteredTimestampedWindowStoreTest
//    {
//        private InternalMockProcessorContext context;

//        private IWindowStore<Bytes, byte[]> innerStoreMock = EasyMock.createNiceMock(IWindowStore);
//        private MeteredTimestampedWindowStore<string, string> store = new MeteredTimestampedWindowStore<>(
//            innerStoreMock,
//            10L, // any size
//            "scope",
//            new MockTime(),
//            Serdes.String(),
//            new ValueAndTimestampSerde<>(new SerdeThatDoesntHandleNull())
//        );
//        private Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG));

//    {
//        EasyMock.expect(innerStoreMock.name()).andReturn("mocked-store").anyTimes();
//    }


//    public void SetUp()
//    {
//        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, "test");

//        context = new InternalMockProcessorContext(
//            TestUtils.GetTempDirectory(),
//            Serdes.String(),
//            Serdes.Long(),
//            streamsMetrics,
//            new StreamsConfig(StreamsTestUtils.getStreamsConfig()),
//            NoOpRecordCollector::new,
//            new ThreadCache(new LogContext("testCache "), 0, streamsMetrics)
//        );
//    }

//    [Xunit.Fact]
//    public void ShouldCloseUnderlyingStore()
//    {
//        innerStoreMock.close();
//        EasyMock.expectLastCall();
//        EasyMock.replay(innerStoreMock);

//        store.Init(context, store);
//        store.close();
//        EasyMock.verify(innerStoreMock);
//    }

//    [Xunit.Fact]
//    public void ShouldNotExceptionIfFetchReturnsNull()
//    {
//        EasyMock.expect(innerStoreMock.Fetch(Bytes.Wrap("a".getBytes()), 0)).andReturn(null);
//        EasyMock.replay(innerStoreMock);

//        store.Init(context, store);
//        Assert.Null(store.Fetch("a", 0));
//    }

//    [Xunit.Fact]
//    public void ShouldNotThrowExceptionIfSerdesCorrectlySetFromProcessorContext()
//    {
//        EasyMock.expect(innerStoreMock.name()).andStubReturn("mocked-store");
//        EasyMock.replay(innerStoreMock);
//        MeteredTimestampedWindowStore<string, long> store = new MeteredTimestampedWindowStore<>(
//            innerStoreMock,
//            10L, // any size
//            "scope",
//            new MockTime(),
//            null,
//            null
//        );
//        store.Init(context, innerStoreMock);

//        try
//        {
//            store.put("key", ValueAndTimestamp.Make(42L, 60000));
//        }
//        catch (StreamsException exception)
//        {
//            if (exception.getCause() is ClassCastException)
//            {
//                Assert.True(false, "Serdes are not correctly set from processor context.");
//            }
//            throw exception;
//        }
//    }

//    [Xunit.Fact]
//    public void ShouldNotThrowExceptionIfSerdesCorrectlySetFromConstructorParameters()
//    {
//        EasyMock.expect(innerStoreMock.name()).andStubReturn("mocked-store");
//        EasyMock.replay(innerStoreMock);
//        MeteredTimestampedWindowStore<string, long> store = new MeteredTimestampedWindowStore<>(
//            innerStoreMock,
//            10L, // any size
//            "scope",
//            new MockTime(),
//            Serdes.String(),
//            new ValueAndTimestampSerde<>(Serdes.Long())
//        );
//        store.Init(context, innerStoreMock);

//        try
//        {
//            store.put("key", ValueAndTimestamp.Make(42L, 60000));
//        }
//        catch (StreamsException exception)
//        {
//            if (exception.getCause() is ClassCastException)
//            {
//                Assert.True(false, "Serdes are not correctly set from constructor parameters.");
//            }
//            throw exception;
//        }
//    }
//}
//}
///*






//*

//*





//*/































