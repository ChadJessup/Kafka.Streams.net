
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public class ValueAndTimestampSerde<V> : Serde<ValueAndTimestamp<V>>
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
            valueAndTimestampSerializer.Configure(configs, isKey);
            valueAndTimestampDeserializer.Configure(configs, isKey);
        }

        public override void Close()
        {
            valueAndTimestampSerializer.Close();
            valueAndTimestampDeserializer.Close();
        }
    }
}
