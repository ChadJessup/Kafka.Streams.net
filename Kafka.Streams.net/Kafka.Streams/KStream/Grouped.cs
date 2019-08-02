﻿using Kafka.Streams.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.KStream
{
    /**
     * The class that is used to capture the key and value {@link Serde}s and set the part of name used for
     * repartition topics when performing {@link KStream#groupBy(KeyValueMapper, Grouped)}, {@link
     * KStream#groupByKey(Grouped)}, or {@link KTable#groupBy(KeyValueMapper, Grouped)} operations.  Note
     * that Kafka Streams does not always create repartition topics for grouping operations.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public class Grouped<K, V> : INamedOperation<Grouped<K, V>>
    {
        protected Serde<K> keySerde;
        protected Serde<V> valueSerde;
        protected string name;


        private Grouped(string name, Serde<K> keySerde, Serde<V> valueSerde)
        {
            this.name = name;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        protected Grouped(Grouped<K, V> grouped)
         : this(grouped.name, grouped.keySerde, grouped.valueSerde)
        {
        }

        /**
         * Create a {@link Grouped} instance with the provided name used as part of the repartition topic if required.
         *
         * @param name the name used for a repartition topic if required
         * @return a new {@link Grouped} configured with the name
         * @see KStream#groupByKey(Grouped)
         * @see KStream#groupBy(KeyValueMapper, Grouped)
         * @see KTable#groupBy(KeyValueMapper, Grouped)
         */
        public static Grouped<K, V> As(string name)
        {
            return new Grouped<K, V>(name, null, null);
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
        public static Grouped<K, V> KeySerde(Serde<K> keySerde)
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
        public static Grouped<K, V> ValueSerde(Serde<V> valueSerde)
        {
            return new Grouped<K, V>(null, null, valueSerde);
        }

        /**
         * Create a {@link Grouped} instance with the provided  name, keySerde, and valueSerde. If the keySerde and/or the valueSerde is
         * {@code null} the default value for the respective serde from config will be used.
         *
         * @param name       the name used as part of the repartition topic name if required
         * @param keySerde   the {@link Serde} used for serializing the key. If {@code null} the default key serde from config will be used
         * @param valueSerde the {@link Serde} used for serializing the value. If {@code null} the default value serde from config will be used
         * @return a new {@link Grouped} configured with the name, keySerde, and valueSerde
         * @see KStream#groupByKey(Grouped)
         * @see KStream#groupBy(KeyValueMapper, Grouped)
         * @see KTable#groupBy(KeyValueMapper, Grouped)
         */
        public static Grouped<K, V> With(string name,
                                                Serde<K> keySerde,
                                                Serde<V> valueSerde)
        {
            return new Grouped<K, V>(name, keySerde, valueSerde);
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
        public static Grouped<K, V> With(Serde<K> keySerde,
                                                Serde<V> valueSerde)
        {
            return new Grouped<K, V>(null, keySerde, valueSerde);
        }

        /**
         * Perform the grouping operation with the name for a repartition topic if required.  Note
         * that Kafka Streams does not always create repartition topics for grouping operations.
         *
         * @param name the name used for the processor name and as part of the repartition topic name if required
         * @return a new {@link Grouped} instance configured with the name
         * */
        public Grouped<K, V> WithName(string name)
        {
            return new Grouped<K, V>(name, keySerde, valueSerde);
        }

        /**
         * Perform the grouping operation using the provided keySerde for serializing the key.
         *
         * @param keySerde {@link Serde} to use for serializing the key. If {@code null} the default key serde from config will be used
         * @return a new {@link Grouped} instance configured with the keySerde
         */
        public Grouped<K, V> WithKeySerde(Serde<K> keySerde)
        {
            return new Grouped<K, V>(name, keySerde, valueSerde);
        }

        /**
         * Perform the grouping operation using the provided valueSerde for serializing the value.
         *
         * @param valueSerde {@link Serde} to use for serializing the value. If {@code null} the default value serde from config will be used
         * @return a new {@link Grouped} instance configured with the valueSerde
         */
        public Grouped<K, V> WithValueSerde(Serde<V> valueSerde)
        {
            return new Grouped<K, V>(name, keySerde, valueSerde);
        }

    }

}
