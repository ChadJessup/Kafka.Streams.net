﻿using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.KStream
{
    /**
     * The {@code Consumed} class is used to define the optional parameters when using {@link StreamsBuilder} to
     * build instances of {@link KStream}, {@link KTable}, and {@link GlobalKTable}.
     * <p>
     * For example, you can read a topic as {@link KStream} with a custom timestamp extractor and specify the corresponding
     * key and value serdes like:
     * <pre>{@code
     * StreamsBuilder builder = new StreamsBuilder();
     * KStream<String, Long> stream = builder.stream(
     *   "topicName",
     *   Consumed.with(Serdes.String(), Serdes.Long())
     *           .withTimestampExtractor(new LogAndSkipOnInvalidTimestamp()));
     * }</pre>
     * Similarly, you can read a topic as {@link KTable} with a custom {@code auto.offset.reset} configuration and force a
     * state store {@link org.apache.kafka.streams.kstream.Materialized materialization} to access the content via
     * interactive queries:
     * <pre>{@code
     * StreamsBuilder builder = new StreamsBuilder();
     * KTable<Integer, Integer> table = builder.table(
     *   "topicName",
     *   Consumed.with(AutoOffsetReset.LATEST),
     *   Materialized.as("queryable-store-name"));
     * }</pre>
     *
     * @param <K> type of record key
     * @param <V> type of record value
     */
    public class Consumed<K, V> : INamedOperation<Consumed<K, V>>
    {

    protected Serde<K> keySerde;
    protected Serde<V> valueSerde;
    protected ITimestampExtractor timestampExtractor;
    protected Topology.AutoOffsetReset resetPolicy;
    protected String processorName;

    private Consumed(Serde<K> keySerde,
                     Serde<V> valueSerde,
                     ITimestampExtractor timestampExtractor,
                     Topology.AutoOffsetReset resetPolicy,
                     String processorName)
    {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.timestampExtractor = timestampExtractor;
        this.resetPolicy = resetPolicy;
        this.processorName = processorName;
    }

    /**
     * Create an instance of {@link Consumed} from an existing instance.
     * @param consumed  the instance of {@link Consumed} to copy
     */
    protected Consumed(Consumed<K, V> consumed)
        : this(consumed.keySerde,
             consumed.valueSerde,
             consumed.timestampExtractor,
             consumed.resetPolicy,
             consumed.processorName
        )
    {
    }

    /**
     * Create an instance of {@link Consumed} with the supplied arguments. {@code null} values are acceptable.
     *
     * @param keySerde           the key serde. If {@code null} the default key serde from config will be used
     * @param valueSerde         the value serde. If {@code null} the default value serde from config will be used
     * @param timestampExtractor the timestamp extractor to used. If {@code null} the default timestamp extractor from config will be used
     * @param resetPolicy        the offset reset policy to be used. If {@code null} the default reset policy from config will be used
     * @param <K>                key type
     * @param <V>                value type
     * @return a new instance of {@link Consumed}
     */
    public static Consumed<K, V> With(Serde<K> keySerde,
                                      Serde<V> valueSerde,
                                      ITimestampExtractor timestampExtractor,
                                      Topology.AutoOffsetReset resetPolicy)
    {
        return new Consumed<K, V>(keySerde, valueSerde, timestampExtractor, resetPolicy, null);

    }

    /**
     * Create an instance of {@link Consumed} with key and value {@link Serde}s.
     *
     * @param keySerde   the key serde. If {@code null} the default key serde from config will be used
     * @param valueSerde the value serde. If {@code null} the default value serde from config will be used
     * @param <K>        key type
     * @param <V>        value type
     * @return a new instance of {@link Consumed}
     */
    public static Consumed<K, V> With(Serde<K> keySerde,
                                      Serde<V> valueSerde)
    {
        return new Consumed<K, V>(keySerde, valueSerde, null, Topology.AutoOffsetReset.UNKNOWN, null);
    }

    /**
     * Create an instance of {@link Consumed} with a {@link TimestampExtractor}.
     *
     * @param timestampExtractor the timestamp extractor to used. If {@code null} the default timestamp extractor from config will be used
     * @param <K>                key type
     * @param <V>                value type
     * @return a new instance of {@link Consumed}
     */
    public static Consumed<K, V> With(ITimestampExtractor timestampExtractor)
    {
        return new Consumed<K, V>(null, null, timestampExtractor, Topology.AutoOffsetReset.UNKNOWN, null);
    }

    /**
     * Create an instance of {@link Consumed} with a {@link org.apache.kafka.streams.Topology.AutoOffsetReset Topology.AutoOffsetReset}.
     *
     * @param resetPolicy the offset reset policy to be used. If {@code null} the default reset policy from config will be used
     * @param <K>         key type
     * @param <V>         value type
     * @return a new instance of {@link Consumed}
     */
    public static Consumed<K, V> With(Topology.AutoOffsetReset resetPolicy)
    {
        return new Consumed<K, V>(null, null, null, resetPolicy, null);
    }

    /**
     * Create an instance of {@link Consumed} with provided processor name.
     *
     * @param processorName the processor name to be used. If {@code null} a default processor name will be generated
     * @param <K>         key type
     * @param <V>         value type
     * @return a new instance of {@link Consumed}
     */
    public static Consumed<K, V> As(string processorName) {
        return new Consumed<K, V>(null, null, null, Topology.AutoOffsetReset.UNKNOWN, processorName);
    }

/**
 * Configure the instance of {@link Consumed} with a key {@link Serde}.
 *
 * @param keySerde the key serde. If {@code null}the default key serde from config will be used
 * @return this
 */
public Consumed<K, V> WithKeySerde(Serde<K> keySerde)
{
    this.keySerde = keySerde;
    return this;
}

/**
 * Configure the instance of {@link Consumed} with a value {@link Serde}.
 *
 * @param valueSerde the value serde. If {@code null} the default value serde from config will be used
 * @return this
 */
public Consumed<K, V> WithValueSerde(Serde<V> valueSerde)
{
    this.valueSerde = valueSerde;
    return this;
}

/**
 * Configure the instance of {@link Consumed} with a {@link TimestampExtractor}.
 *
 * @param timestampExtractor the timestamp extractor to used. If {@code null} the default timestamp extractor from config will be used
 * @return this
 */
public Consumed<K, V> WithTimestampExtractor(ITimestampExtractor timestampExtractor)
{
    this.timestampExtractor = timestampExtractor;
    return this;
}

/**
 * Configure the instance of {@link Consumed} with a {@link org.apache.kafka.streams.Topology.AutoOffsetReset Topology.AutoOffsetReset}.
 *
 * @param resetPolicy the offset reset policy to be used. If {@code null} the default reset policy from config will be used
 * @return this
 */
public Consumed<K, V> WithOffsetResetPolicy(Topology.AutoOffsetReset resetPolicy)
{
    this.resetPolicy = resetPolicy;
    return this;
}

/**
 * Configure the instance of {@link Consumed} with a processor name.
 *
 * @param processorName the processor name to be used. If {@code null} a default processor name will be generated
 * @return this
 */
    public Consumed<K, V> WithName(string processorName)
{
    this.processorName = processorName;
    return this;
}

    public override bool Equals(object o)
{
    if (this == o)
    {
        return true;
    }
    if (o == null || this.GetType() != o.GetType())
    {
        return false;
    }

    var consumed = (Consumed <K, V>) o;
    return object.Equals(keySerde, consumed.keySerde) &&
           object.Equals(valueSerde, consumed.valueSerde) &&
           object.Equals(timestampExtractor, consumed.timestampExtractor) &&
           resetPolicy == consumed.resetPolicy;
}

    public override int GetHashCode()
{
    return (keySerde, valueSerde, timestampExtractor, resetPolicy).GetHashCode();
}
}

}