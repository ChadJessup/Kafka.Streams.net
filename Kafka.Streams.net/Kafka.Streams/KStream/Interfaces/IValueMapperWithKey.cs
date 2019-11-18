namespace Kafka.Streams.KStream.Interfaces
{
    /**
     * The {@code ValueMapperWithKey} interface for mapping a value to a new value of arbitrary type.
     * This is a stateless record-by-record operation, i.e, {@link #apply(object, object)} is invoked individually for each
     * record of a stream (cf. {@link ValueTransformer} for stateful value transformation).
     * If {@code ValueMapperWithKey} is applied to a {@link org.apache.kafka.streams.KeyValue key-value pair} record the
     * record's key is preserved.
     * Note that the key is read-only and should not be modified, as this can lead to corrupt partitioning.
     * If a record's key and value should be modified {@link KeyValueMapper} can be used.
     *
     * @param  key type
     * @param  value type
     * @param mapped value type
     * @see KeyValueMapper
     * @see ValueTransformer
     * @see ValueTransformerWithKey
     * @see KStream#mapValues(ValueMapper)
     * @see KStream#mapValues(ValueMapperWithKey)
     * @see KStream#flatMapValues(ValueMapper)
     * @see KStream#flatMapValues(ValueMapperWithKey)
     * @see KTable#mapValues(ValueMapper)
     * @see KTable#mapValues(ValueMapperWithKey)
     */

    public interface IValueMapperWithKey<K, V, VR>
    {
        /**
         * Map the given [key and value] to a new value.
         *
         * @param readOnlyKey the read-only key
         * @param value       the value to be mapped
         * @return the new value
         */
        VR apply(K readOnlyKey, V value);
    }
}