﻿namespace Kafka.Streams.Interfaces
{
    /**
    * The {@code KeyValueMapper} interface for mapping a {@link KeyValuePair key-value pair} to a new value of arbitrary type.
    * For example, it can be used to
    * <ul>
    * <li>map from an input {@link KeyValuePair} pair to an output {@link KeyValuePair} pair with different key and/or value type
    *     (for this case output type {@code VR == }{@link KeyValuePair KeyValuePair&lt;NewKeyType,NewValueType&gt;})</li>
    * <li>map from an input record to a new key (with arbitrary key type as specified by {@code VR})</li>
    * </ul>
    * This is a stateless record-by-record operation, i.e, {@link #apply(object, object)} is invoked individually for each
    * record of a stream (cf. {@link Transformer} for stateful record transformation).
    * {@code KeyValueMapper} is a generalization of {@link ValueMapper}.
    *
    * @param  key type
    * @param  value type
    * @param mapped value type
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
    public delegate TMappedValue KeyValueMapper<in TKey, in TValue, out TMappedValue>(TKey key, TValue value);
    public delegate TMappedValue ValueMapperWithKey<in TKey, in TValue, out TMappedValue>(TKey key, TValue value);

    public delegate bool FilterPredicate<in TKey, in TValue>(TKey key, TValue value);

    public delegate TMappedValue ValueMapper<in TValue, out TMappedValue>(TValue value);

    public delegate TJoinedValue ValueJoiner<in TValue1, in TValue2, out TJoinedValue>(TValue1 value1, TValue2 value2);
    public delegate TJoinedValue ValueJoiner<in TValue, out TJoinedValue>(TValue value1);

    //    public interface IKeyValueMapper<K, V, VR>
    //    {
    //        /**
    //         * Map a record with the given key and value to a new value.
    //         *
    //         * @param key   the key of the record
    //         * @param value the value of the record
    //         * @return the new value
    //         */
    //        VR Apply(K key, V value);
    //    }
}
