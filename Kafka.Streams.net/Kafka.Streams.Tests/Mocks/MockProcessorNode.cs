using Kafka.Streams.Nodes;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Temporary;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Tests.Mocks
{
    public class MockProcessorNode<K, V> : ProcessorNode<K, V>
    {
        private static readonly string NAME = "MOCK-PROCESS-";
        private static int INDEX = 1;

        public MockProcessor<K, V> mockProcessor;

        public bool closed;
        public bool initialized;

        public MockProcessorNode(TimeSpan scheduleInterval)
            : this(scheduleInterval, PunctuationType.STREAM_TIME)
        {
        }

        public MockProcessorNode(TimeSpan scheduleInterval, PunctuationType punctuationType)
            : this(new MockProcessor<K, V>(punctuationType, scheduleInterval))
        {
        }

        public MockProcessorNode()
            : this(new MockProcessor<K, V>())
        {
        }

        private MockProcessorNode(MockProcessor<K, V> mockProcessor)
            : base(null, NAME + ++INDEX, mockProcessor, Collections.emptySet<string>())
        {

            this.mockProcessor = mockProcessor;
        }

        public override void Init(IInternalProcessorContext context)
        {
            base.Init(context);
            initialized = true;
        }

        public override void Process(K key, V value)
        {
            processor.Process(key, value);
        }

        public override void Close()
        {
            base.Close();
            this.closed = true;
        }
    }
}
