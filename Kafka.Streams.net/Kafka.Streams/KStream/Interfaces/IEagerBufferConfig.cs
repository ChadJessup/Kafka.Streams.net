namespace Kafka.Streams.KStream.Interfaces
{
    /**
    * Marker interface for a buffer configuration that will strictly enforce size constraints
    * (bytes and/or number of records) on the buffer, so it is suitable for reducing duplicate
    * results downstream, but does not promise to eliminate them entirely.
    */
    public interface IEagerBufferConfig : IBufferConfig<IEagerBufferConfig>
    {
    }
}
