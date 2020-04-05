//using Kafka.Streams.Tasks;
//using System;
//using System.Collections.Generic;

//namespace Kafka.Streams.Tests.Processor.Internals.assignment
//{
//    /*






//    *

//    *





//    */













//    public class SubscriptionInfoTest
//    {
//        private UUID processId = UUID.randomUUID();
//        private HashSet<TaskId> activeTasks = new HashSet<>(Array.asList(
//            new TaskId(0, 0),
//            new TaskId(0, 1),
//            new TaskId(1, 0)));
//        private HashSet<TaskId> standbyTasks = new HashSet<>(Array.asList(
//            new TaskId(1, 1),
//            new TaskId(2, 0)));

//        private const string IGNORED_USER_ENDPOINT = "ignoredUserEndpoint:80";

//        [Fact]
//        public void ShouldUseLatestSupportedVersionByDefault()
//        {
//            SubscriptionInfo info = new SubscriptionInfo(processId, activeTasks, standbyTasks, "localhost:80");
//            Assert.Equal(SubscriptionInfo.LATEST_SUPPORTED_VERSION, info.version());
//        }

//        [Fact]// (expected = ArgumentException)
//        public void ShouldThrowForUnknownVersion1()
//        {
//            new SubscriptionInfo(0, processId, activeTasks, standbyTasks, "localhost:80");
//        }

//        [Fact]// (expected = ArgumentException)
//        public void ShouldThrowForUnknownVersion2()
//        {
//            new SubscriptionInfo(SubscriptionInfo.LATEST_SUPPORTED_VERSION + 1, processId, activeTasks, standbyTasks, "localhost:80");
//        }

//        [Fact]
//        public void ShouldEncodeAndDecodeVersion1()
//        {
//            SubscriptionInfo info = new SubscriptionInfo(1, processId, activeTasks, standbyTasks, IGNORED_USER_ENDPOINT);
//            SubscriptionInfo expectedInfo = new SubscriptionInfo(1, SubscriptionInfo.UNKNOWN, processId, activeTasks, standbyTasks, null);
//            Assert.Equal(expectedInfo, SubscriptionInfo.decode(info.encode()));
//        }

//        [Fact]
//        public void ShouldEncodeAndDecodeVersion2()
//        {
//            SubscriptionInfo info = new SubscriptionInfo(2, processId, activeTasks, standbyTasks, "localhost:80");
//            SubscriptionInfo expectedInfo = new SubscriptionInfo(2, SubscriptionInfo.UNKNOWN, processId, activeTasks, standbyTasks, "localhost:80");
//            Assert.Equal(expectedInfo, SubscriptionInfo.decode(info.encode()));
//        }

//        [Fact]
//        public void ShouldEncodeAndDecodeVersion3()
//        {
//            SubscriptionInfo info = new SubscriptionInfo(3, processId, activeTasks, standbyTasks, "localhost:80");
//            SubscriptionInfo expectedInfo = new SubscriptionInfo(3, SubscriptionInfo.LATEST_SUPPORTED_VERSION, processId, activeTasks, standbyTasks, "localhost:80");
//            Assert.Equal(expectedInfo, SubscriptionInfo.decode(info.encode()));
//        }

//        [Fact]
//        public void ShouldEncodeAndDecodeVersion4()
//        {
//            SubscriptionInfo info = new SubscriptionInfo(4, processId, activeTasks, standbyTasks, "localhost:80");
//            SubscriptionInfo expectedInfo = new SubscriptionInfo(4, SubscriptionInfo.LATEST_SUPPORTED_VERSION, processId, activeTasks, standbyTasks, "localhost:80");
//            Assert.Equal(expectedInfo, SubscriptionInfo.decode(info.encode()));
//        }

//        [Fact]
//        public void ShouldAllowToDecodeFutureSupportedVersion()
//        {
//            SubscriptionInfo info = SubscriptionInfo.decode(EncodeFutureVersion());
//            Assert.Equal(SubscriptionInfo.LATEST_SUPPORTED_VERSION + 1, info.version());
//            Assert.Equal(SubscriptionInfo.LATEST_SUPPORTED_VERSION + 1, info.latestSupportedVersion());
//        }

//        private ByteBuffer EncodeFutureVersion()
//        {
//            ByteBuffer buf = new ByteBuffer().Allocate(4 /* used version */
//                                                       + 4 /* supported version */);
//            buf.putInt(SubscriptionInfo.LATEST_SUPPORTED_VERSION + 1);
//            buf.putInt(SubscriptionInfo.LATEST_SUPPORTED_VERSION + 1);
//            return buf;
//        }

//    }
//}
///*






//*

//*





//*/













