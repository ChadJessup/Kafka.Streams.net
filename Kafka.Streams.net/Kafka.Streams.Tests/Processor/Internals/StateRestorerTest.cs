/*






 *

 *





 */
















public class StateRestorerTest {

    private static long OFFSET_LIMIT = 50;
    private MockRestoreCallback callback = new MockRestoreCallback();
    private MockStateRestoreListener reportingListener = new MockStateRestoreListener();
    private CompositeRestoreListener compositeRestoreListener = new CompositeRestoreListener(callback);
    private StateRestorer restorer = new StateRestorer(
        new TopicPartition("topic", 1),
        compositeRestoreListener,
        null,
        OFFSET_LIMIT,
        true,
        "storeName",
        identity());

    
    public void setUp() {
        compositeRestoreListener.setUserRestoreListener(reportingListener);
    }

    [Xunit.Fact]
    public void shouldCallRestoreOnRestoreCallback() {
        restorer.restore(Collections.singletonList(new ConsumeResult<>("", 0, 0L, new byte[0], new byte[0])));
        Assert.Equal(callback.restored.Count, (1));
    }

    [Xunit.Fact]
    public void shouldBeCompletedIfRecordOffsetGreaterThanEndOffset() {
        Assert.True(restorer.hasCompleted(11, 10));
    }

    [Xunit.Fact]
    public void shouldBeCompletedIfRecordOffsetGreaterThanOffsetLimit() {
        Assert.True(restorer.hasCompleted(51, 100));
    }

    [Xunit.Fact]
    public void shouldBeCompletedIfEndOffsetAndRecordOffsetAreZero() {
        Assert.True(restorer.hasCompleted(0, 0));
    }

    [Xunit.Fact]
    public void shouldBeCompletedIfOffsetAndOffsetLimitAreZero() {
        StateRestorer restorer = new StateRestorer(
            new TopicPartition("topic", 1),
            compositeRestoreListener,
            null,
            0,
            true,
            "storeName",
            identity());
        Assert.True(restorer.hasCompleted(0, 10));
    }

    [Xunit.Fact]
    public void shouldSetRestoredOffsetToMinOfLimitAndOffset() {
        restorer.setRestoredOffset(20);
        Assert.Equal(restorer.restoredOffset(), (20L));
        restorer.setRestoredOffset(100);
        Assert.Equal(restorer.restoredOffset(), (OFFSET_LIMIT));
    }

    [Xunit.Fact]
    public void shouldSetStartingOffsetToMinOfLimitAndOffset() {
        restorer.setStartingOffset(20);
        Assert.Equal(restorer.startingOffset(), (20L));
        restorer.setRestoredOffset(100);
        Assert.Equal(restorer.restoredOffset(), (OFFSET_LIMIT));
    }

    [Xunit.Fact]
    public void shouldReturnCorrectNumRestoredRecords() {
        restorer.setStartingOffset(20);
        restorer.setRestoredOffset(40);
        Assert.Equal(restorer.restoredNumRecords(), (20L));
        restorer.setRestoredOffset(100);
        Assert.Equal(restorer.restoredNumRecords(), (OFFSET_LIMIT - 20));
    }
}
