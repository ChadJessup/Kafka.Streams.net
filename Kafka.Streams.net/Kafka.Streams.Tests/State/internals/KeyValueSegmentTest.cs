/*






 *

 *





 */




















public class KeyValueSegmentTest {

    [Xunit.Fact]
    public void shouldDeleteStateDirectoryOnDestroy() {// throws Exception
        KeyValueSegment segment = new KeyValueSegment("segment", "window", 0L);
        string directoryPath = TestUtils.tempDirectory().getAbsolutePath();
        File directory = new File(directoryPath);

        ProcessorContext mockContext = mock(ProcessorContext);
        expect(mockContext.appConfigs()).andReturn(emptyMap());
        expect(mockContext.stateDir()).andReturn(directory);
        replay(mockContext);

        segment.openDB(mockContext);

        Assert.True(new File(directoryPath, "window").exists());
        Assert.True(new File(directoryPath + File.separator + "window", "segment").exists());
        Assert.True(new File(directoryPath + File.separator + "window", "segment").list().Length > 0);
        segment.destroy();
        Assert.False(new File(directoryPath + File.separator + "window", "segment").exists());
        Assert.True(new File(directoryPath, "window").exists());
    }

    [Xunit.Fact]
    public void shouldBeEqualIfIdIsEqual() {
        KeyValueSegment segment = new KeyValueSegment("anyName", "anyName", 0L);
        KeyValueSegment segmentSameId = new KeyValueSegment("someOtherName", "someOtherName", 0L);
        KeyValueSegment segmentDifferentId = new KeyValueSegment("anyName", "anyName", 1L);

        Assert.Equal(segment, (segment));
        Assert.Equal(segment, (segmentSameId));
        Assert.Equal(segment, not(equalTo(segmentDifferentId)));
        Assert.Equal(segment, not(equalTo(null)));
        Assert.Equal(segment, not(equalTo("anyName")));
    }

    [Xunit.Fact]
    public void shouldHashOnSegmentIdOnly() {
        KeyValueSegment segment = new KeyValueSegment("anyName", "anyName", 0L);
        KeyValueSegment segmentSameId = new KeyValueSegment("someOtherName", "someOtherName", 0L);
        KeyValueSegment segmentDifferentId = new KeyValueSegment("anyName", "anyName", 1L);

        HashSet<KeyValueSegment> set = new HashSet<>();
        Assert.True(set.add(segment));
        Assert.False(set.add(segmentSameId));
        Assert.True(set.add(segmentDifferentId));
    }

    [Xunit.Fact]
    public void shouldCompareSegmentIdOnly() {
        KeyValueSegment segment1 = new KeyValueSegment("a", "C", 50L);
        KeyValueSegment segment2 = new KeyValueSegment("b", "B", 100L);
        KeyValueSegment segment3 = new KeyValueSegment("c", "A", 0L);

        Assert.Equal(segment1.compareTo(segment1), (0));
        Assert.Equal(segment1.compareTo(segment2), (-1));
        Assert.Equal(segment2.compareTo(segment1), (1));
        Assert.Equal(segment1.compareTo(segment3), (1));
        Assert.Equal(segment3.compareTo(segment1), (-1));
        Assert.Equal(segment2.compareTo(segment3), (1));
        Assert.Equal(segment3.compareTo(segment2), (-1));
    }
}
