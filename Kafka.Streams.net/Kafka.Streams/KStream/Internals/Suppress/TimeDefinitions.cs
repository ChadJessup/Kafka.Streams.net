using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using System;

namespace Kafka.Streams.KStream.Internals.Suppress
{
    public class RecordTimeDefintion<K> : ITimeDefinition<K>
    {
        private static readonly RecordTimeDefintion<K> INSTANCE = new RecordTimeDefintion<K>();

        private RecordTimeDefintion() { }


        public static RecordTimeDefintion<K> Instance()
        {
            return RecordTimeDefintion<K>.INSTANCE;
        }

        public DateTime Time(IProcessorContext context, K key)
        {
            return context.Timestamp;
        }

        public TimeDefinitionType Type()
        {
            return TimeDefinitionType.RECORD_TIME;
        }
    }
}
