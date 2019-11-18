namespace Kafka.Streams.KStream
{
    /**
     * The {@code ValueJoiner} interface for joining two values into a new value of arbitrary type.
     * This is a stateless operation, i.e, {@link #apply(object, object)} is invoked individually for each joining
     * record-pair of a {@link KStream}-{@link KStream}, {@link KStream}-{@link KTable}, or {@link KTable}-{@link KTable}
     * join.
     *
     * @param first value type
     * @param second value type
     * @param joined value type
     * @see KStream#join(KStream, ValueJoiner, JoinWindows)
     * @see KStream#join(KStream, ValueJoiner, JoinWindows, Joined)
     * @see KStream#leftJoin(KStream, ValueJoiner, JoinWindows)
     * @see KStream#leftJoin(KStream, ValueJoiner, JoinWindows, Joined)
     * @see KStream#outerJoin(KStream, ValueJoiner, JoinWindows)
     * @see KStream#outerJoin(KStream, ValueJoiner, JoinWindows, Joined)
     * @see KStream#join(KTable, ValueJoiner)
     * @see KStream#join(KTable, ValueJoiner, Joined)
     * @see KStream#leftJoin(KTable, ValueJoiner)
     * @see KStream#leftJoin(KTable, ValueJoiner, Joined)
     * @see KTable#join(KTable, ValueJoiner)
     * @see KTable#leftJoin(KTable, ValueJoiner)
     * @see KTable#outerJoin(KTable, ValueJoiner)
     */
    public interface IValueJoiner<V1, V2, out VR>
    {
        /**
         * Return a joined value consisting of {@code value1} and {@code value2}.
         *
         * @param value1 the first value for joining
         * @param value2 the second value for joining
         * @return the joined value
         */
        VR apply(V1 value1, V2 value2);
        VR apply(V1 value1);
    }
}
