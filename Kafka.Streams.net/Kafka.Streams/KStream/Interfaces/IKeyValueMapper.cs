namespace Kafka.Streams.KStream
{
    /**
     * The {@code KeyValueMapper} interface for mapping a {@link KeyValue key-value pair} to a new value of arbitrary type.
     * For example, it can be used to
     * <ul>
     * <li>map from an input {@link KeyValue} pair to an output {@link KeyValue} pair with different key and/or value type
     *     (for this case output type {@code VR == }{@link KeyValue KeyValue&lt;NewKeyType,NewValueType&gt;})</li>
     * <li>map from an input record to a new key (with arbitrary key type as specified by {@code VR})</li>
     * </ul>
     * This is a stateless record-by-record operation, i.e, {@link #apply(Object, Object)} is invoked individually for each
     * record of a stream (cf. {@link Transformer} for stateful record transformation).
     * {@code KeyValueMapper} is a generalization of {@link ValueMapper}.
     *
     * @param <K>  key type
     * @param <V>  value type
     * @param <VR> mapped value type
     * @see ValueMapper
     * @see Transformer
     * @see KStream#map(KeyValueMapper)
     * @see KStream#flatMap(KeyValueMapper)
     * @see KStream#selectKey(KeyValueMapper)
     * @see KStream#groupBy(KeyValueMapper)
     * @see KStream#groupBy(KeyValueMapper, Grouped)
     * @see KTable#groupBy(KeyValueMapper)
     * @see KTable#groupBy(KeyValueMapper, Grouped)
     * @see KTable#toStream(KeyValueMapper)
     */
    public interface IKeyValueMapper<K, V, VR>
    {
        /**
         * Map a record with the given key and value to a new value.
         *
         * @param key   the key of the record
         * @param value the value of the record
         * @return the new value
         */
        VR apply(K key, V value);
    }
}