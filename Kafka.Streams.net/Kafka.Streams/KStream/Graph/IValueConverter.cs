namespace Kafka.Streams.KStream.Internals.Graph
{
    public interface IValueConverter<V>
    {
        public VR ConvertValueTo<VR>();
    }
}