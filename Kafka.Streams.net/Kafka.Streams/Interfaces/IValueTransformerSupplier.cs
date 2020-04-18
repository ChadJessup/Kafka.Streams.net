using Kafka.Streams.KStream.Interfaces;

namespace Kafka.Streams.Interfaces
{
    /**
    * A {@code ValueTransformerSupplier} interface which can create one or more {@link ValueTransformer} instances.
    *
    * @param <V>  value type
    * @param <VR> transformed value type
    * @see ValueTransformer
    * @see ValueTransformerWithKey
    * @see ValueTransformerWithKeySupplier
    * @see KStream#transformValues(ValueTransformerSupplier, String...)
    * @see KStream#transformValues(ValueTransformerWithKeySupplier, String...)
    * @see Transformer
    * @see TransformerSupplier
    * @see KStream#transform(TransformerSupplier, String...)
    */
    public interface IValueTransformerSupplier<V, VR>
    {
        /**
         * Return a new {@link ValueTransformer} instance.
         *
         * @return a new {@link ValueTransformer} instance.
         */
        IValueTransformer<V, VR> Get();
    }
}
