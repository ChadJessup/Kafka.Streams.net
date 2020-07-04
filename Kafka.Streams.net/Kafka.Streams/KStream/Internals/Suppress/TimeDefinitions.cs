using Kafka.Streams.Processors.Interfaces;
using System;

namespace Kafka.Streams.KStream.Internals.Suppress
{
    public static class RecordTimeDefintion
    {
        public static ITimeDefinition<K> Instance<K>()
        {
            return RecordTimeDefintion<K>.INSTANCE;
        }
    }

    public class RecordTimeDefintion<K> : ITimeDefinition<K>
    {
        internal static readonly RecordTimeDefintion<K> INSTANCE = new RecordTimeDefintion<K>();

        private RecordTimeDefintion() { }

        public DateTime Time(IProcessorContext context, K key)
        {
            if (context is null)
            {
                throw new ArgumentNullException(nameof(context));
            }

            return context.Timestamp;
        }

        public TimeDefinitionType Type()
        {
            return TimeDefinitionType.RECORD_TIME;
        }
    }
}
