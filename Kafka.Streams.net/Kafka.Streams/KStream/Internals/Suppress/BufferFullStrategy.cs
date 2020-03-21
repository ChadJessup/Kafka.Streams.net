namespace Kafka.Streams.KStream.Internals.Suppress
{
    public enum BufferFullStrategy
    {
        EMIT,
        SPILL_TO_DISK,
        SHUT_DOWN
    }
}
