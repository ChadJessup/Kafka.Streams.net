using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using System;

namespace Kafka.Streams.KStream
{
    public static class Joined
    {
        /**
         * Create an instance of {@code Joined} with key, value, and otherValue {@link Serde} instances.
         * {@code null} values are accepted and will be replaced by the default serdes as defined in config.
         *
         * @param keySerde        the key serde to use. If {@code null} the default key serde from config will be used
         * @param valueSerde      the value serde to use. If {@code null} the default value serde from config will be used
         * @param otherValueSerde the otherValue serde to use. If {@code null} the default value serde from config will be used
         * @param             key type
         * @param             value type
         * @param            other value type
         * @return new {@code Joined} instance with the provided serdes
         */
        public static Joined<K, V, VO> With<K, V, VO>(
            ISerde<K>? keySerde,
            ISerde<V>? valueSerde,
            ISerde<VO>? otherValueSerde)
        {
            return new Joined<K, V, VO>(
                keySerde,
                valueSerde,
                otherValueSerde,
                null);
        }

        /**
         * Create an instance of {@code Joined} with key, value, and otherValue {@link Serde} instances.
         * {@code null} values are accepted and will be replaced by the default serdes as defined in
         * config.
         *
         * @param keySerde the key serde to use. If {@code null} the default key serde from config will be
         * used
         * @param valueSerde the value serde to use. If {@code null} the default value serde from config
         * will be used
         * @param otherValueSerde the otherValue serde to use. If {@code null} the default value serde
         * from config will be used
         * @param Name the Name used as the base for naming components of the join including any
         * repartition topics
         * @param key type
         * @param value type
         * @param other value type
         * @return new {@code Joined} instance with the provided serdes
         */
        public static Joined<K, V, VO> With<K, V, VO>(
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
            ISerde<VO> otherValueSerde,
            string Name)
        {
            if (keySerde is null)
            {
                throw new ArgumentNullException(nameof(keySerde));
            }

            if (valueSerde is null)
            {
                throw new ArgumentNullException(nameof(valueSerde));
            }

            if (otherValueSerde is null)
            {
                throw new ArgumentNullException(nameof(otherValueSerde));
            }

            if (string.IsNullOrEmpty(Name))
            {
                throw new ArgumentException("message", nameof(Name));
            }

            return new Joined<K, V, VO>(
                keySerde,
                valueSerde,
                otherValueSerde,
                Name);
        }

        /**
         * Create an instance of {@code Joined} with  a key {@link Serde}.
         * {@code null} values are accepted and will be replaced by the default key serde as defined in config.
         *
         * @param keySerde the key serde to use. If {@code null} the default key serde from config will be used
         * @param      key type
         * @param      value type
         * @param     other value type
         * @return new {@code Joined} instance configured with the keySerde
         */
        public static Joined<K, V, VO> JoinedKeySerde<K, V, VO>(ISerde<K> keySerde)
        {
            if (keySerde is null)
            {
                throw new ArgumentNullException(nameof(keySerde));
            }

            return new Joined<K, V, VO>(keySerde, null, null, null);
        }

        /**
         * Create an instance of {@code Joined} with a value {@link Serde}.
         * {@code null} values are accepted and will be replaced by the default value serde as defined in config.
         *
         * @param valueSerde the value serde to use. If {@code null} the default value serde from config will be used
         * @param        key type
         * @param        value type
         * @param       other value type
         * @return new {@code Joined} instance configured with the valueSerde
         */
        public static Joined<K, V, VO> JoinedValueSerde<K, V, VO>(ISerde<V> valueSerde)
        {
            if (valueSerde is null)
            {
                throw new ArgumentNullException(nameof(valueSerde));
            }

            return new Joined<K, V, VO>(null, valueSerde, null, null);
        }

        /**
         * Create an instance of {@code Joined} with an other value {@link Serde}.
         * {@code null} values are accepted and will be replaced by the default value serde as defined in config.
         *
         * @param otherValueSerde the otherValue serde to use. If {@code null} the default value serde from config will be used
         * @param             key type
         * @param             value type
         * @param            other value type
         * @return new {@code Joined} instance configured with the otherValueSerde
         */
        public static Joined<K, V, VO> OtherValueSerde<K, V, VO>(ISerde<VO> otherValueSerde)
        {
            if (otherValueSerde is null)
            {
                throw new ArgumentNullException(nameof(otherValueSerde));
            }

            return new Joined<K, V, VO>(null, null, otherValueSerde, null);
        }

        /**
         * Create an instance of {@code Joined} with base Name for All components of the join, this may
         * include any repartition topics created to complete the join.
         *
         * @param Name the Name used as the base for naming components of the join including any
         * repartition topics
         * @param key type
         * @param value type
         * @param other value type
         * @return new {@code Joined} instance configured with the Name
         *
         */
        public static Joined<K, V, VO> As<K, V, VO>(string Name)
        {
            if (string.IsNullOrWhiteSpace(Name))
            {
                throw new ArgumentException("message", nameof(Name));
            }

            return new Joined<K, V, VO>(null, null, null, Name);
        }
    }

    /**
     * The {@code Joined} represents optional params that can be passed to
     * {@link KStream#join}, {@link KStream#leftJoin}, and  {@link KStream#outerJoin} operations.
     */
    public class Joined<K, V, VO> : INamedOperation<Joined<K, V, VO>>
    {
        public ISerde<K>? KeySerde { get; }
        public ISerde<V>? ValueSerde { get; }
        public ISerde<VO>? OtherValueSerde { get; }
        public string? Name { get; }

        public Joined(
            ISerde<K>? keySerde,
            ISerde<V>? valueSerde,
            ISerde<VO>? otherValueSerde,
            string? Name)
        {
            this.KeySerde = keySerde;
            this.ValueSerde = valueSerde;
            this.OtherValueSerde = otherValueSerde;
            this.Name = Name;
        }

        protected Joined(Joined<K, V, VO> joined)
            : this(joined?.KeySerde, joined?.ValueSerde, joined?.OtherValueSerde, joined?.Name)
        {
        }

        /**
         * Set the key {@link Serde} to be used. Null values are accepted and will be replaced by the default
         * key serde as defined in config
         *
         * @param keySerde the key serde to use. If null the default key serde from config will be used
         * @return new {@code Joined} instance configured with the {@code Name}
         */
        public Joined<K, V, VO> WithKeySerde(ISerde<K> keySerde)
        {
            if (keySerde is null)
            {
                throw new ArgumentNullException(nameof(keySerde));
            }

            return new Joined<K, V, VO>(keySerde, this.ValueSerde, this.OtherValueSerde, this.Name);
        }

        /**
         * Set the value {@link Serde} to be used. Null values are accepted and will be replaced by the default
         * value serde as defined in config
         *
         * @param valueSerde the value serde to use. If null the default value serde from config will be used
         * @return new {@code Joined} instance configured with the {@code valueSerde}
         */
        public Joined<K, V, VO> WithValueSerde(ISerde<V> valueSerde)
        {
            if (valueSerde is null)
            {
                throw new ArgumentNullException(nameof(valueSerde));
            }

            return new Joined<K, V, VO>(this.KeySerde, valueSerde, this.OtherValueSerde, this.Name);
        }

        /**
         * Set the otherValue {@link Serde} to be used. Null values are accepted and will be replaced by the default
         * value serde as defined in config
         *
         * @param otherValueSerde the otherValue serde to use. If null the default value serde from config will be used
         * @return new {@code Joined} instance configured with the {@code valueSerde}
         */
        public Joined<K, V, VO> WithOtherValueSerde(ISerde<VO> otherValueSerde)
        {
            if (otherValueSerde is null)
            {
                throw new ArgumentNullException(nameof(otherValueSerde));
            }

            return new Joined<K, V, VO>(this.KeySerde, this.ValueSerde, otherValueSerde, this.Name);
        }

        /**
         * Set the base Name used for All components of the join, this may include any repartition topics
         * created to complete the join.
         *
         * @param Name the Name used as the base for naming components of the join including any
         * repartition topics
         * @return new {@code Joined} instance configured with the {@code Name}
         */
        public Joined<K, V, VO> WithName(string Name)
        {
            if (string.IsNullOrWhiteSpace(Name))
            {
                throw new ArgumentException("message", nameof(Name));
            }

            return new Joined<K, V, VO>(this.KeySerde, this.ValueSerde, this.OtherValueSerde, Name);
        }
    }
}
