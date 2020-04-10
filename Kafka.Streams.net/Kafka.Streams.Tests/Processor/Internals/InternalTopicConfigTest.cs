//using Kafka.Streams.Processors.Internals;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    public class InternalTopicConfigTest
//    {
//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowIfNameIsNull()
//        {
//            new RepartitionTopicConfig(null, Collections.< string, string > emptyMap());
//        }

//        [Fact]// (expected = InvalidTopicException)
//        public void ShouldThrowIfNameIsInvalid()
//        {
//            new RepartitionTopicConfig("foo bar baz", Collections.< string, string > emptyMap());
//        }

//        [Fact]
//        public void ShouldAugmentRetentionMsWithWindowedChangelog()
//        {
//            WindowedChangelogTopicConfig topicConfig = new WindowedChangelogTopicConfig("Name", Collections.< string, string > emptyMap());
//            topicConfig.setRetentionMs(10);
//            Assert.Equal("30", topicConfig.getProperties(Collections.< string, string > emptyMap(), 20).Get(TopicConfig.RETENTION_MS_CONFIG));
//        }

//        [Fact]
//        public void ShouldUseSuppliedConfigs()
//        {
//            Dictionary<string, string> configs = new HashMap<>();
//            configs.Put("retention.ms", "1000");
//            configs.Put("retention.bytes", "10000");

//            UnwindowedChangelogTopicConfig topicConfig = new UnwindowedChangelogTopicConfig("Name", configs);

//            Dictionary<string, string> properties = topicConfig.getProperties(Collections.< string, string > emptyMap(), 0);
//            Assert.Equal("1000", properties.Get("retention.ms"));
//            Assert.Equal("10000", properties.Get("retention.bytes"));
//        }

//        [Fact]
//        public void ShouldUseSuppliedConfigsForRepartitionConfig()
//        {
//            Dictionary<string, string> configs = new HashMap<>();
//            configs.Put("retention.ms", "1000");
//            RepartitionTopicConfig topicConfig = new RepartitionTopicConfig("Name", configs);
//            Assert.Equal("1000", topicConfig.getProperties(Collections.< string, string > emptyMap(), 0).Get(TopicConfig.RETENTION_MS_CONFIG));
//        }
//    }
//}
