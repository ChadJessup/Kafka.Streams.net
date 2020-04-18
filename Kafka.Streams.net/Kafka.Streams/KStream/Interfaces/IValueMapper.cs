//namespace Kafka.Streams.KStream.Interfaces
//{
//    /**
//     * The {@code ValueMapper} interface for mapping a value to a new value of arbitrary type.
//     * This is a stateless record-by-record operation, i.e, {@link #apply(object)} is invoked individually for each record
//     * of a stream (cf. {@link ValueTransformer} for stateful value transformation).
//     * If {@code ValueMapper} is applied to a {@link org.apache.kafka.streams.KeyValuePair key-value pair} record the record's
//     * key is preserved.
//     * If a record's key and value should be modified {@link KeyValueMapper} can be used.
//     *
//     * @param  value type
//     * @param mapped value type
//     * @see KeyValueMapper
//     * @see ValueTransformer
//     * @see ValueTransformerWithKey
//     * @see KStream#mapValues(ValueMapper)
//     * @see KStream#mapValues(ValueMapperWithKey)
//     * @see KStream#flatMapValues(ValueMapper)
//     * @see KStream#flatMapValues(ValueMapperWithKey)
//     * @see KTable#mapValues(ValueMapper)
//     * @see KTable#mapValues(ValueMapperWithKey)
//     */
//    public interface IValueMapper<V, VR>
//    {
//        /**
//         * Map the given value to a new value.
//         *
//         * @param value the value to be mapped
//         * @return the new value
//         */
//        VR Apply(V value);
//    }
//}
