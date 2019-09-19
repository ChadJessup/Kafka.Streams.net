﻿using Confluent.Kafka;

namespace Kafka.Streams.Processor.Internals
{
    public class RecordInfo
    {
        public RecordQueue queue { get; set; }

        public ProcessorNode node()
        {
            return null;// queue.source();
        }

        public TopicPartition partition()
        {
            return queue.partition;
        }
    }
}