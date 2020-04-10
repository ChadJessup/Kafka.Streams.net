using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public interface IValueAndTimestampSerde
    {
    }

    public interface IValueAndTimestampSerde<V> : IValueAndTimestampSerde
    {
    }

    public class ValueAndTimestampSerde<V> : Serde<IValueAndTimestamp<V>>, IValueAndTimestampSerde<V>
    {
        private readonly ValueAndTimestampSerializer<V> valueAndTimestampSerializer;
        private readonly ValueAndTimestampDeserializer<V> valueAndTimestampDeserializer;

        public ValueAndTimestampSerde(ISerde<V> valueSerde)
            : base(
                 serializer: new ValueAndTimestampSerializer<V>(valueSerde.Serializer),
                 deserializer: new ValueAndTimestampDeserializer<V>(valueSerde.Deserializer))
        {
            valueSerde = valueSerde ?? throw new ArgumentNullException(nameof(valueSerde));
        }

        public override void Configure(
            IDictionary<string, string> configs,
            bool isKey)
        {
            //this.Serializer.Configure(configs, isKey);
            this.valueAndTimestampSerializer.Configure(configs, isKey);
            this.valueAndTimestampDeserializer.Configure(configs, isKey);
        }

        public override void Close()
        {
            this.valueAndTimestampSerializer.Close();
            this.valueAndTimestampDeserializer.Close();
        }
    }
}
