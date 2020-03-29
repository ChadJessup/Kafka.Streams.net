/*






 *

 *





 */















public class BufferValueTest {
    [Xunit.Fact]
    public void ShouldDeduplicateNullValues() {
        BufferValue bufferValue = new BufferValue(null, null, null, null);
        assertSame(bufferValue.priorValue(), bufferValue.oldValue());
    }

    [Xunit.Fact]
    public void ShouldDeduplicateIndenticalValues() {
        byte[] bytes = {(byte) 0};
        BufferValue bufferValue = new BufferValue(bytes, bytes, null, null);
        assertSame(bufferValue.priorValue(), bufferValue.oldValue());
    }

    [Xunit.Fact]
    public void ShouldDeduplicateEqualValues() {
        BufferValue bufferValue = new BufferValue(new byte[] {(byte) 0}, new byte[] {(byte) 0}, null, null);
        assertSame(bufferValue.priorValue(), bufferValue.oldValue());
    }

    [Xunit.Fact]
    public void ShouldStoreDifferentValues() {
        byte[] priorValue = {(byte) 0};
        byte[] oldValue = {(byte) 1};
        BufferValue bufferValue = new BufferValue(priorValue, oldValue, null, null);
        assertSame(priorValue, bufferValue.priorValue());
        assertSame(oldValue, bufferValue.oldValue());
        Assert.NotEqual(bufferValue.priorValue(), bufferValue.oldValue());
    }

    [Xunit.Fact]
    public void ShouldStoreDifferentValuesWithPriorNull() {
        byte[] priorValue = null;
        byte[] oldValue = {(byte) 1};
        BufferValue bufferValue = new BufferValue(priorValue, oldValue, null, null);
        assertNull(bufferValue.priorValue());
        assertSame(oldValue, bufferValue.oldValue());
        Assert.NotEqual(bufferValue.priorValue(), bufferValue.oldValue());
    }

    [Xunit.Fact]
    public void ShouldStoreDifferentValuesWithOldNull() {
        byte[] priorValue = {(byte) 0};
        byte[] oldValue = null;
        BufferValue bufferValue = new BufferValue(priorValue, oldValue, null, null);
        assertSame(priorValue, bufferValue.priorValue());
        assertNull(bufferValue.oldValue());
        Assert.NotEqual(bufferValue.priorValue(), bufferValue.oldValue());
    }

    [Xunit.Fact]
    public void ShouldAccountForDeduplicationInSizeEstimate() {
        ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        Assert.Equal(25L, new BufferValue(null, null, null, context).residentMemorySizeEstimate());
        Assert.Equal(26L, new BufferValue(new byte[] {(byte) 0}, null, null, context).residentMemorySizeEstimate());
        Assert.Equal(26L, new BufferValue(null, new byte[] {(byte) 0}, null, context).residentMemorySizeEstimate());
        Assert.Equal(26L, new BufferValue(new byte[] {(byte) 0}, new byte[] {(byte) 0}, null, context).residentMemorySizeEstimate());
        Assert.Equal(27L, new BufferValue(new byte[] {(byte) 0}, new byte[] {(byte) 1}, null, context).residentMemorySizeEstimate());

        // new value should get counted, but doesn't get deduplicated
        Assert.Equal(28L, new BufferValue(new byte[] {(byte) 0}, new byte[] {(byte) 1}, new byte[] {(byte) 0}, context).residentMemorySizeEstimate());
    }

    [Xunit.Fact]
    public void ShouldSerializeNulls() {
        ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        byte[] serializedContext = context.serialize();
        byte[] bytes = new BufferValue(null, null, null, context).serialize(0).array();
        byte[] withoutContext = Array.copyOfRange(bytes, serializedContext.Length, bytes.Length);

        Assert.Equal(withoutContext, is(ByteBuffer.allocate(int.BYTES * 3).putInt(-1).putInt(-1).putInt(-1).array()));
    }

    [Xunit.Fact]
    public void ShouldSerializePrior() {
        ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        byte[] serializedContext = context.serialize();
        byte[] priorValue = {(byte) 5};
        byte[] bytes = new BufferValue(priorValue, null, null, context).serialize(0).array();
        byte[] withoutContext = Array.copyOfRange(bytes, serializedContext.Length, bytes.Length);

        Assert.Equal(withoutContext, is(ByteBuffer.allocate(int.BYTES * 3 + 1).putInt(1).put(priorValue).putInt(-1).putInt(-1).array()));
    }

    [Xunit.Fact]
    public void ShouldSerializeOld() {
        ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        byte[] serializedContext = context.serialize();
        byte[] oldValue = {(byte) 5};
        byte[] bytes = new BufferValue(null, oldValue, null, context).serialize(0).array();
        byte[] withoutContext = Array.copyOfRange(bytes, serializedContext.Length, bytes.Length);

        Assert.Equal(withoutContext, is(ByteBuffer.allocate(int.BYTES * 3 + 1).putInt(-1).putInt(1).put(oldValue).putInt(-1).array()));
    }

    [Xunit.Fact]
    public void ShouldSerializeNew() {
        ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        byte[] serializedContext = context.serialize();
        byte[] newValue = {(byte) 5};
        byte[] bytes = new BufferValue(null, null, newValue, context).serialize(0).array();
        byte[] withoutContext = Array.copyOfRange(bytes, serializedContext.Length, bytes.Length);

        Assert.Equal(withoutContext, is(ByteBuffer.allocate(int.BYTES * 3 + 1).putInt(-1).putInt(-1).putInt(1).put(newValue).array()));
    }

    [Xunit.Fact]
    public void ShouldCompactDuplicates() {
        ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        byte[] serializedContext = context.serialize();
        byte[] duplicate = {(byte) 5};
        byte[] bytes = new BufferValue(duplicate, duplicate, null, context).serialize(0).array();
        byte[] withoutContext = Array.copyOfRange(bytes, serializedContext.Length, bytes.Length);

        Assert.Equal(withoutContext, is(ByteBuffer.allocate(int.BYTES * 3 + 1).putInt(1).put(duplicate).putInt(-2).putInt(-1).array()));
    }

    [Xunit.Fact]
    public void ShouldDeserializePrior() {
        ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        byte[] serializedContext = context.serialize();
        byte[] priorValue = {(byte) 5};
        ByteBuffer serialValue =
            ByteBuffer
                .allocate(serializedContext.Length + int.BYTES * 3 + priorValue.Length)
                .put(serializedContext).putInt(1).put(priorValue).putInt(-1).putInt(-1);
        serialValue.position(0);

        BufferValue deserialize = BufferValue.deserialize(serialValue);
        Assert.Equal(deserialize, is(new BufferValue(priorValue, null, null, context)));
    }

    [Xunit.Fact]
    public void ShouldDeserializeOld() {
        ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        byte[] serializedContext = context.serialize();
        byte[] oldValue = {(byte) 5};
        ByteBuffer serialValue =
            ByteBuffer
                .allocate(serializedContext.Length + int.BYTES * 3 + oldValue.Length)
                .put(serializedContext).putInt(-1).putInt(1).put(oldValue).putInt(-1);
        serialValue.position(0);

        Assert.Equal(BufferValue.deserialize(serialValue), is(new BufferValue(null, oldValue, null, context)));
    }

    [Xunit.Fact]
    public void ShouldDeserializeNew() {
        ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        byte[] serializedContext = context.serialize();
        byte[] newValue = {(byte) 5};
        ByteBuffer serialValue =
            ByteBuffer
                .allocate(serializedContext.Length + int.BYTES * 3 + newValue.Length)
                .put(serializedContext).putInt(-1).putInt(-1).putInt(1).put(newValue);
        serialValue.position(0);

        Assert.Equal(BufferValue.deserialize(serialValue), is(new BufferValue(null, null, newValue, context)));
    }

    [Xunit.Fact]
    public void ShouldDeserializeCompactedDuplicates() {
        ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        byte[] serializedContext = context.serialize();
        byte[] duplicate = {(byte) 5};
        ByteBuffer serialValue =
            ByteBuffer
                .allocate(serializedContext.Length + int.BYTES * 3 + duplicate.Length)
                .put(serializedContext).putInt(1).put(duplicate).putInt(-2).putInt(-1);
        serialValue.position(0);

        BufferValue bufferValue = BufferValue.deserialize(serialValue);
        Assert.Equal(bufferValue, is(new BufferValue(duplicate, duplicate, null, context)));
        assertSame(bufferValue.priorValue(), bufferValue.oldValue());
    }
}
