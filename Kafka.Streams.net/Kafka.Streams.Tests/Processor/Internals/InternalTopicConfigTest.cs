/*






 *

 *





 */












public class InternalTopicConfigTest {

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowIfNameIsNull() {
        new RepartitionTopicConfig(null, Collections.<string, string>emptyMap());
    }

    [Xunit.Fact]// (expected = InvalidTopicException)
    public void ShouldThrowIfNameIsInvalid() {
        new RepartitionTopicConfig("foo bar baz", Collections.<string, string>emptyMap());
    }

    [Xunit.Fact]
    public void ShouldAugmentRetentionMsWithWindowedChangelog() {
        WindowedChangelogTopicConfig topicConfig = new WindowedChangelogTopicConfig("name", Collections.<string, string>emptyMap());
        topicConfig.setRetentionMs(10);
        Assert.Equal("30", topicConfig.getProperties(Collections.<string, string>emptyMap(), 20).get(TopicConfig.RETENTION_MS_CONFIG));
    }

    [Xunit.Fact]
    public void ShouldUseSuppliedConfigs() {
        Dictionary<string, string> configs = new HashMap<>();
        configs.put("retention.ms", "1000");
        configs.put("retention.bytes", "10000");

        UnwindowedChangelogTopicConfig topicConfig = new UnwindowedChangelogTopicConfig("name", configs);

        Dictionary<string, string> properties = topicConfig.getProperties(Collections.<string, string>emptyMap(), 0);
        Assert.Equal("1000", properties.get("retention.ms"));
        Assert.Equal("10000", properties.get("retention.bytes"));
    }

    [Xunit.Fact]
    public void ShouldUseSuppliedConfigsForRepartitionConfig() {
        Dictionary<string, string> configs = new HashMap<>();
        configs.put("retention.ms", "1000");
        RepartitionTopicConfig topicConfig = new RepartitionTopicConfig("name", configs);
        Assert.Equal("1000", topicConfig.getProperties(Collections.<string, string>emptyMap(), 0).get(TopicConfig.RETENTION_MS_CONFIG));
    }
}