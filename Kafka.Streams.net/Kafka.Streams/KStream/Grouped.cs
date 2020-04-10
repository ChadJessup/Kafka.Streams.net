using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;

namespace Kafka.Streams.KStream
{
    public static class Grouped
    {
        /**
         * Create a {@link Grouped} instance with the provided Name used as part of the repartition topic if required.
         *
         * @param Name the Name used for a repartition topic if required
         * @return a new {@link Grouped} configured with the Name
         * @see KStream#groupByKey(Grouped)
         * @see KStream#groupBy(KeyValueMapper, Grouped)
         * @see KTable#groupBy(KeyValueMapper, Grouped)
         */
        public static Grouped<K, V> As<K, V>(string Name)
        {
            return new Grouped<K, V>(Name, null, null);
        }

        /**
         * Create a {@link Grouped} instance with the provided keySerde. If {@code null} the default key serde from config will be used.
         *
         * @param keySerde the Serde used for serializing the key. If {@code null} the default key serde from config will be used
         * @return a new {@link Grouped} configured with the keySerde
         * @see KStream#groupByKey(Grouped)
         * @see KStream#groupBy(KeyValueMapper, Grouped)
         * @see KTable#groupBy(KeyValueMapper, Grouped)
         */
        public static Grouped<K, V> KeySerde<K, V>(ISerde<K> keySerde)
        {
            return new Grouped<K, V>(null, keySerde, null);
        }

        /**
         * Create a {@link Grouped} instance with the provided valueSerde.  If {@code null} the default value serde from config will be used.
         *
         * @param valueSerde the {@link Serde} used for serializing the value. If {@code null} the default value serde from config will be used
         * @return a new {@link Grouped} configured with the valueSerde
         * @see KStream#groupByKey(Grouped)
         * @see KStream#groupBy(KeyValueMapper, Grouped)
         * @see KTable#groupBy(KeyValueMapper, Grouped)
         */
        public static Grouped<K, V> ValueSerde<K, V>(ISerde<V> valueSerde)
        {
            return new Grouped<K, V>(null, null, valueSerde);
        }

        /**
         * Create a {@link Grouped} instance with the provided  Name, keySerde, and valueSerde. If the keySerde and/or the valueSerde is
         * {@code null} the default value for the respective serde from config will be used.
         *
         * @param Name       the Name used as part of the repartition topic Name if required
         * @param keySerde   the {@link Serde} used for serializing the key. If {@code null} the default key serde from config will be used
         * @param valueSerde the {@link Serde} used for serializing the value. If {@code null} the default value serde from config will be used
         * @return a new {@link Grouped} configured with the Name, keySerde, and valueSerde
         * @see KStream#groupByKey(Grouped)
         * @see KStream#groupBy(KeyValueMapper, Grouped)
         * @see KTable#groupBy(KeyValueMapper, Grouped)
         */
        public static Grouped<K, V> With<K, V>(
            string Name,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
        {
            return new Grouped<K, V>(Name, keySerde, valueSerde);
        }

        /**
         * Create a {@link Grouped} instance with the provided keySerde and valueSerde.  If the keySerde and/or the valueSerde is
         * {@code null} the default value for the respective serde from config will be used.
         *
         * @param keySerde   the {@link Serde} used for serializing the key. If {@code null} the default key serde from config will be used
         * @param valueSerde the {@link Serde} used for serializing the value. If {@code null} the default value serde from config will be used
         * @return a new {@link Grouped} configured with the keySerde, and valueSerde
         * @see KStream#groupByKey(Grouped)
         * @see KStream#groupBy(KeyValueMapper, Grouped)
         * @see KTable#groupBy(KeyValueMapper, Grouped)
         */
        public static Grouped<K, V> With<K, V>(
            ISerde<K>? keySerde,
            ISerde<V>? valueSerde)
        {
            return new Grouped<K, V>(null, keySerde, valueSerde);
        }
    }

    /**
     * The that is used to capture the key and value {@link Serde}s and set the part of Name used for
     * repartition topics when performing {@link KStream#groupBy(KeyValueMapper, Grouped)}, {@link
     * KStream#groupByKey(Grouped)}, or {@link KTable#groupBy(KeyValueMapper, Grouped)} operations.  Note
     * that Kafka Streams does not always create repartition topics for grouping operations.
     *
     * @param the key type
     * @param the value type
     */
    public class Grouped<K, V> : INamedOperation<Grouped<K, V>>
    {
        public ISerde<K>? KeySerde { get; private set; }
        public ISerde<V>? ValueSerde { get; private set; }
        public string? Name { get; }

        public Grouped(string? Name, ISerde<K>? keySerde, ISerde<V>? valueSerde)
        {
            this.Name = Name;
            this.KeySerde = keySerde;
            this.ValueSerde = valueSerde;
        }

        public Grouped(Grouped<K, V> grouped)
         : this(grouped?.Name, grouped?.KeySerde, grouped?.ValueSerde)
        {
        }

        /**
         * Perform the grouping operation with the Name for a repartition topic if required.  Note
         * that Kafka Streams does not always create repartition topics for grouping operations.
         *
         * @param Name the Name used for the processor Name and as part of the repartition topic Name if required
         * @return a new {@link Grouped} instance configured with the Name
         * */
        public Grouped<K, V> WithName(string Name)
        {
            return new Grouped<K, V>(Name, this.KeySerde, this.ValueSerde);
        }

        /**
         * Perform the grouping operation using the provided keySerde for serializing the key.
         *
         * @param keySerde {@link Serde} to use for serializing the key. If {@code null} the default key serde from config will be used
         * @return a new {@link Grouped} instance configured with the keySerde
         */
        public Grouped<K, V> WithKeySerde(ISerde<K> keySerde)
        {
            return new Grouped<K, V>(this.Name, keySerde, this.ValueSerde);
        }

        /**
         * Perform the grouping operation using the provided valueSerde for serializing the value.
         *
         * @param valueSerde {@link Serde} to use for serializing the value. If {@code null} the default value serde from config will be used
         * @return a new {@link Grouped} instance configured with the valueSerde
         */
        public Grouped<K, V> WithValueSerde(ISerde<V> valueSerde)
        {
            return new Grouped<K, V>(this.Name, this.KeySerde, valueSerde);
        }
    }
}
