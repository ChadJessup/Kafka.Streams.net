
namespace Kafka.Streams.KStream
{
    /**
     * A {@code TransformerSupplier} interface which can create one or more {@link Transformer} instances.
     *
     * @param key type
     * @param value type
     * @param {@link org.apache.kafka.streams.KeyValuePair KeyValuePair} return type (both key and value type can be set
     *            arbitrarily)
     * @see Transformer
     * @see KStream#transform(TransformerSupplier, string...)
     * @see ValueTransformer
     * @see ValueTransformerSupplier
     * @see KStream#transformValues(ValueTransformerSupplier, string...)
     */
    public interface ITransformerSupplier<K, V, R>
    {
        /**
         * Return a new {@link Transformer} instance.
         *
         * @return a new {@link Transformer} instance
         */
        ITransformer<K, V, R> Get();
    }
}
