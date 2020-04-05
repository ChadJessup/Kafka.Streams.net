//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    /*






//    *

//    *





//    */




















//    public class StateRestoreCallbackAdapterTest
//    {
//        [Fact]// (expected = UnsupportedOperationException)
//        public void ShouldThrowOnRestoreAll()
//        {
//            adapt(mock(StateRestoreCallback)).restoreAll(null);
//        }

//        [Fact]// (expected = UnsupportedOperationException)
//        public void ShouldThrowOnRestore()
//        {
//            adapt(mock(StateRestoreCallback)).restore(null, null);
//        }

//        [Fact]
//        public void ShouldPassRecordsThrough()
//        {
//            ArrayList<ConsumeResult<byte[], byte[]>> actual = new ArrayList<>();
//            RecordBatchingStateRestoreCallback callback = actual::addAll;

//            RecordBatchingStateRestoreCallback adapted = adapt(callback);

//            byte[] key1 = { 1 };
//            byte[] value1 = { 2 };
//            byte[] key2 = { 3 };
//            byte[] value2 = { 4 };

//            List<ConsumeResult<byte[], byte[]>> recordList = asList(
//                new ConsumeResult<>("topic1", 0, 0L, key1, value1),
//                new ConsumeResult<>("topic2", 1, 1L, key2, value2)
//            );

//            adapted.restoreBatch(recordList);

//            validate(actual, recordList);
//        }

//        [Fact]
//        public void ShouldConvertToKeyValueBatches()
//        {
//            ArrayList<KeyValuePair<byte[], byte[]>> actual = new ArrayList<>();
//            BatchingStateRestoreCallback callback = new BatchingStateRestoreCallback()
//            {


//            public void restoreAll(Collection<KeyValuePair<byte[], byte[]>> records)
//            {
//                actual.addAll(records);
//            }


//            public void restore(byte[] key, byte[] value)
//            {
//                // unreachable
//            }
//        };

//        RecordBatchingStateRestoreCallback adapted = adapt(callback);
//        readonly byte[] key1 = { 1 };
//        readonly byte[] value1 = { 2 };
//        readonly byte[] key2 = { 3 };
//        readonly byte[] value2 = { 4 };
//        adapted.restoreBatch(asList(
    
//                new ConsumeResult<>("topic1", 0, 0L, key1, value1),
//            new ConsumeResult<>("topic2", 1, 1L, key2, value2)
//        ));

//        Assert.Equal(
//            actual,
//            (asList(
    
//                    KeyValuePair.Create(key1, value1),
//                KeyValuePair.Create(key2, value2)
//            ))
//        );
//    }

//    [Fact]
//    public void ShouldConvertToKeyValue()
//    {
//        ArrayList<KeyValuePair<byte[], byte[]>> actual = new ArrayList<>();
//        StateRestoreCallback callback = (key, value) => actual.Add(KeyValuePair.Create(key, value));

//        RecordBatchingStateRestoreCallback adapted = adapt(callback);

//        byte[] key1 = { 1 };
//        byte[] value1 = { 2 };
//        byte[] key2 = { 3 };
//        byte[] value2 = { 4 };
//        adapted.restoreBatch(asList(
//            new ConsumeResult<>("topic1", 0, 0L, key1, value1),
//            new ConsumeResult<>("topic2", 1, 1L, key2, value2)
//        ));

//        Assert.Equal(
//            actual,
//            (asList(
//                KeyValuePair.Create(key1, value1),
//                KeyValuePair.Create(key2, value2)
//            ))
//        );
//    }

//    private void Validate(List<ConsumeResult<byte[], byte[]>> actual,
//                          List<ConsumeResult<byte[], byte[]>> expected)
//    {
//        Assert.Equal(actual.Count, (expected.Count));
//        for (int i = 0; i < actual.Count; i++)
//        {
//            ConsumeResult<byte[], byte[]> actual1 = actual.Get(i);
//            ConsumeResult<byte[], byte[]> expected1 = expected.Get(i);
//            Assert.Equal(actual1.Topic, (expected1.Topic));
//            Assert.Equal(actual1.Partition, (expected1.Partition));
//            Assert.Equal(actual1.Offset, (expected1.Offset));
//            Assert.Equal(actual1.Key, (expected1.Key));
//            Assert.Equal(actual1.Value, (expected1.Value));
//            Assert.Equal(actual1.Timestamp, (expected1.Timestamp));
//            Assert.Equal(actual1.Headers, (expected1.Headers));
//        }
//    }


//}}
///*






//*

//*





//*/






















