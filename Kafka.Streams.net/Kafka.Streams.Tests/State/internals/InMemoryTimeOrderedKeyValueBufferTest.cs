/*






 *

 *





 */




public class InMemoryTimeOrderedKeyValueBufferTest {

    [Xunit.Fact]
    public void bufferShouldAllowCacheEnablement() {
        new InMemoryTimeOrderedKeyValueBuffer.Builder<>(null, null, null).withCachingEnabled();
    }

    [Xunit.Fact]
    public void bufferShouldAllowCacheDisablement() {
        new InMemoryTimeOrderedKeyValueBuffer.Builder<>(null, null, null).withCachingDisabled();
    }
}
