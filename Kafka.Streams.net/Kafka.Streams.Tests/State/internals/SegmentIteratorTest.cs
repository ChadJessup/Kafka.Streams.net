/*






 *

 *





 */
























public class SegmentIteratorTest {

    private KeyValueSegment segmentOne = new KeyValueSegment("one", "one", 0);
    private KeyValueSegment segmentTwo = new KeyValueSegment("two", "window", 1);
    private HasNextCondition hasNextCondition = Iterator::hasNext;

    private SegmentIterator<KeyValueSegment> iterator = null;

    
    public void Before() {
        InternalMockProcessorContext context = new InternalMockProcessorContext(
                TestUtils.tempDirectory(),
                Serdes.String(),
                Serdes.String(),
                new NoOpRecordCollector(),
                new ThreadCache(
                    new LogContext("testCache "),
                    0,
                    new MockStreamsMetrics(new Metrics())));
        segmentOne.openDB(context);
        segmentTwo.openDB(context);
        segmentOne.put(Bytes.wrap("a".getBytes()), "1".getBytes());
        segmentOne.put(Bytes.wrap("b".getBytes()), "2".getBytes());
        segmentTwo.put(Bytes.wrap("c".getBytes()), "3".getBytes());
        segmentTwo.put(Bytes.wrap("d".getBytes()), "4".getBytes());
    }

    
    public void CloseSegments() {
        if (iterator != null) {
            iterator.close();
            iterator = null;
        }
        segmentOne.close();
        segmentTwo.close();
    }

    [Xunit.Fact]
    public void ShouldIterateOverAllSegments() {
        iterator = new SegmentIterator<>(
            Array.asList(segmentOne, segmentTwo).iterator(),
            hasNextCondition,
            Bytes.wrap("a".getBytes()),
            Bytes.wrap("z".getBytes()));

        Assert.True(iterator.hasNext());
        Assert.Equal("a", new string(iterator.peekNextKey().get()));
        Assert.Equal(KeyValuePair.Create("a", "1"), toStringKeyValue(iterator.next()));

        Assert.True(iterator.hasNext());
        Assert.Equal("b", new string(iterator.peekNextKey().get()));
        Assert.Equal(KeyValuePair.Create("b", "2"), toStringKeyValue(iterator.next()));

        Assert.True(iterator.hasNext());
        Assert.Equal("c", new string(iterator.peekNextKey().get()));
        Assert.Equal(KeyValuePair.Create("c", "3"), toStringKeyValue(iterator.next()));

        Assert.True(iterator.hasNext());
        Assert.Equal("d", new string(iterator.peekNextKey().get()));
        Assert.Equal(KeyValuePair.Create("d", "4"), toStringKeyValue(iterator.next()));

        Assert.False(iterator.hasNext());
    }

    [Xunit.Fact]
    public void ShouldNotThrowExceptionOnHasNextWhenStoreClosed() {
        iterator = new SegmentIterator<>(
            Collections.singletonList(segmentOne).iterator(),
            hasNextCondition,
            Bytes.wrap("a".getBytes()),
            Bytes.wrap("z".getBytes()));

        iterator.currentIterator = segmentOne.all();
        segmentOne.close();
        Assert.False(iterator.hasNext());
    }

    [Xunit.Fact]
    public void ShouldOnlyIterateOverSegmentsInRange() {
        iterator = new SegmentIterator<>(
            Array.asList(segmentOne, segmentTwo).iterator(),
            hasNextCondition,
            Bytes.wrap("a".getBytes()),
            Bytes.wrap("b".getBytes()));

        Assert.True(iterator.hasNext());
        Assert.Equal("a", new string(iterator.peekNextKey().get()));
        Assert.Equal(KeyValuePair.Create("a", "1"), toStringKeyValue(iterator.next()));

        Assert.True(iterator.hasNext());
        Assert.Equal("b", new string(iterator.peekNextKey().get()));
        Assert.Equal(KeyValuePair.Create("b", "2"), toStringKeyValue(iterator.next()));

        Assert.False(iterator.hasNext());
    }

    [Xunit.Fact]// (expected = NoSuchElementException)
    public void ShouldThrowNoSuchElementOnPeekNextKeyIfNoNext() {
        iterator = new SegmentIterator<>(
            Array.asList(segmentOne, segmentTwo).iterator(),
            hasNextCondition,
            Bytes.wrap("f".getBytes()),
            Bytes.wrap("h".getBytes()));

        iterator.peekNextKey();
    }

    [Xunit.Fact]// (expected = NoSuchElementException)
    public void ShouldThrowNoSuchElementOnNextIfNoNext() {
        iterator = new SegmentIterator<>(
            Array.asList(segmentOne, segmentTwo).iterator(),
            hasNextCondition,
            Bytes.wrap("f".getBytes()),
            Bytes.wrap("h".getBytes()));

        iterator.next();
    }

    private KeyValuePair<string, string> ToStringKeyValue(KeyValuePair<Bytes, byte[]> binaryKv) {
        return KeyValuePair.Create(new string(binaryKv.key.get()), new string(binaryKv.value));
    }
}
