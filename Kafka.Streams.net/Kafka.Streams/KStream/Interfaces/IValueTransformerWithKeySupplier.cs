namespace Kafka.Streams.KStream
{
    /**
     * @param  key type
     * @param  value type
     * @param transformed value type
     * @see ValueTransformer
     * @see ValueTransformerWithKey
     * @see KStream#transformValues(ValueTransformerSupplier, string...)
     * @see KStream#transformValues(ValueTransformerWithKeySupplier, string...)
     * @see Transformer
     * @see TransformerSupplier
     * @see KStream#transform(TransformerSupplier, string...)
     */
    public interface IValueTransformerWithKeySupplier<K, V, VR>
    {
        IValueTransformerWithKey<K, V, VR> Get();
    }
}