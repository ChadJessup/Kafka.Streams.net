/*






 *

 *





 */
















/**
 * Tests local state store and global application cleanup.
 */

public class ResetIntegrationTest : AbstractResetIntegrationTest {

    
    public static EmbeddedKafkaCluster CLUSTER;

    private static string TEST_ID = "reset-integration-test";

    static {
        Properties brokerProps = new Properties();
        // we double the value passed to `time.sleep` in each iteration in one of the map functions, so we disable
        // expiration of connections by the brokers to avoid errors when `AdminClient` sends requests after potentially
        // very long sleep times
        brokerProps.put(KafkaConfig$.MODULE$.ConnectionsMaxIdleMsProp(), -1L);
        CLUSTER = new EmbeddedKafkaCluster(1, brokerProps);
    }

    
    Dictionary<string, object> getClientSslConfig() {
        return null;
    }

    
    public void before() {// throws Exception
        testId = TEST_ID;
        cluster = CLUSTER;
        prepareTest();
    }

    
    public void after() {// throws Exception
        cleanupTest();
    }

    [Xunit.Fact]
    public void testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic() {// throws Exception
        base.testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic();
    }

    [Xunit.Fact]
    public void testReprocessingFromScratchAfterResetWithIntermediateUserTopic() {// throws Exception
        base.testReprocessingFromScratchAfterResetWithIntermediateUserTopic();
    }

    [Xunit.Fact]
    public void testReprocessingFromFileAfterResetWithoutIntermediateUserTopic() {// throws Exception
        base.testReprocessingFromFileAfterResetWithoutIntermediateUserTopic();
    }

    [Xunit.Fact]
    public void testReprocessingFromDateTimeAfterResetWithoutIntermediateUserTopic() {// throws Exception
        base.testReprocessingFromDateTimeAfterResetWithoutIntermediateUserTopic();
    }

    [Xunit.Fact]
    public void testReprocessingByDurationAfterResetWithoutIntermediateUserTopic() {// throws Exception
        base.testReprocessingByDurationAfterResetWithoutIntermediateUserTopic();
    }

    [Xunit.Fact]
    public void shouldNotAllowToResetWhileStreamsRunning() {// throws Exception
        base.shouldNotAllowToResetWhileStreamsIsRunning();
    }

    [Xunit.Fact]
    public void shouldNotAllowToResetWhenInputTopicAbsent() {// throws Exception
        base.shouldNotAllowToResetWhenInputTopicAbsent();
    }

    [Xunit.Fact]
    public void shouldNotAllowToResetWhenIntermediateTopicAbsent() {// throws Exception
        base.shouldNotAllowToResetWhenIntermediateTopicAbsent();
    }
}
