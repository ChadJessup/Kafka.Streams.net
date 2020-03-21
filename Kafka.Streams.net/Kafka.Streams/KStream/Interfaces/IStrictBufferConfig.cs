namespace Kafka.Streams.KStream.Interfaces
{
    /**
     * Marker interface for a buffer configuration that is "strict" in the sense that it will strictly
     * enforce the time bound and never emit early.
     */
    public interface IStrictBufferConfig : IBufferConfig<IStrictBufferConfig>
    {

    }
}
