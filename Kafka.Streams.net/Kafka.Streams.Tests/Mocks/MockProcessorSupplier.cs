using Kafka.Streams.Processors;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Mocks
{
    public class MockProcessorSupplier<K, V> : IProcessorSupplier<K, V>
    {
        private readonly TimeSpan scheduleInterval;
        private readonly PunctuationType punctuationType;
        private readonly List<MockProcessor<K, V>> processors = new List<MockProcessor<K, V>>();

        public MockProcessorSupplier()
            : this(TimeSpan.FromMilliseconds(-1L))
        {
        }

        public MockProcessorSupplier(TimeSpan scheduleInterval)
            : this(scheduleInterval, PunctuationType.STREAM_TIME)
        {
        }

        public MockProcessorSupplier(TimeSpan scheduleInterval, PunctuationType punctuationType)
        {
            this.scheduleInterval = scheduleInterval;
            this.punctuationType = punctuationType;
        }

        public IKeyValueProcessor<K, V> Get()
        {
            var processor = new MockProcessor<K, V>(this.punctuationType, this.scheduleInterval);
            this.processors.Add(processor);
            return processor;
        }

        IKeyValueProcessor IProcessorSupplier.Get() => this.Get();

        // get the captured processor assuming that only one processor gets returned from this supplier
        public MockProcessor<K, V> TheCapturedProcessor()
        {
            return this.CapturedProcessors(1)[0];
        }

        public int CapturedProcessorsCount()
        {
            return this.processors.Count;
        }

        // get the captured processors with the expected number
        public List<MockProcessor<K, V>> CapturedProcessors(int expectedNumberOfProcessors)
        {
            Assert.Equal(expectedNumberOfProcessors, this.processors.Count);

            return this.processors;
        }
    }
}
