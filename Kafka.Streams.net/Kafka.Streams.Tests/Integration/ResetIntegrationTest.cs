/*






 *

 *





 */
















/**
 * Tests local state store and global application cleanup.
 */

public class ResetIntegrationTest : AbstractResetIntegrationTest {

    
    public static EmbeddedKafkaCluster CLUSTER;

    private static readonly string TEST_ID = "reset-integration-test";

    static {
        Properties brokerProps = new Properties();
        // we double the value passed to `time.sleep` in each iteration in one of the map functions, so we disable
        // expiration of connections by the brokers to avoid errors when `AdminClient` sends requests after potentially
        // very long sleep times
        brokerProps.put(KafkaConfig$.MODULE$.ConnectionsMaxIdleMsProp(), -1L);
        CLUSTER = new EmbeddedKafkaCluster(1, brokerProps);
    }

    
    Dictionary<string, object> GetClientSslConfig() {
        return null;
    }

    
    public void Before() {// throws Exception
        testId = TEST_ID;
        cluster = CLUSTER;
        prepareTest();
    }

    
    public void After() {// throws Exception
        cleanupTest();
    }

    [Xunit.Fact]
    public void TestReprocessingFromScratchAfterResetWithoutIntermediateUserTopic() {// throws Exception
        base.testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic();
    }

    [Xunit.Fact]
    public void TestReprocessingFromScratchAfterResetWithIntermediateUserTopic() {// throws Exception
        base.testReprocessingFromScratchAfterResetWithIntermediateUserTopic();
    }

    [Xunit.Fact]
    public void TestReprocessingFromFileAfterResetWithoutIntermediateUserTopic() {// throws Exception
        base.testReprocessingFromFileAfterResetWithoutIntermediateUserTopic();
    }

    [Xunit.Fact]
    public void TestReprocessingFromDateTimeAfterResetWithoutIntermediateUserTopic() {// throws Exception
        base.testReprocessingFromDateTimeAfterResetWithoutIntermediateUserTopic();
    }

    [Xunit.Fact]
    public void TestReprocessingByDurationAfterResetWithoutIntermediateUserTopic() {// throws Exception
        base.testReprocessingByDurationAfterResetWithoutIntermediateUserTopic();
    }

    [Xunit.Fact]
    public void ShouldNotAllowToResetWhileStreamsRunning() {// throws Exception
        base.shouldNotAllowToResetWhileStreamsIsRunning();
    }

    [Xunit.Fact]
    public void ShouldNotAllowToResetWhenInputTopicAbsent() {// throws Exception
        base.shouldNotAllowToResetWhenInputTopicAbsent();
    }

    [Xunit.Fact]
    public void ShouldNotAllowToResetWhenIntermediateTopicAbsent() {// throws Exception
        base.shouldNotAllowToResetWhenIntermediateTopicAbsent();
    }
}
