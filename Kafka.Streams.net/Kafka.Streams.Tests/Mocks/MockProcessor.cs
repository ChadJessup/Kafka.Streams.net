using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Moq;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Mocks
{
    public class MockProcessor<K, V> : AbstractProcessor<K, V>
    {
        public List<KeyValueTimestamp<K, V>> processed = new List<KeyValueTimestamp<K, V>>();
        public Dictionary<K, IValueAndTimestamp<V>> LastValueAndTimestampPerKey = new Dictionary<K, IValueAndTimestamp<V>>();

        public List<DateTime> punctuatedStreamTime = new List<DateTime>();
        public List<DateTime> punctuatedSystemTime = new List<DateTime>();


        private readonly PunctuationType punctuationType;
        private readonly TimeSpan scheduleInterval;

        private bool commitRequested = false;
        private ICancellable scheduleCancellable;

        public MockProcessor(
            PunctuationType punctuationType,
            TimeSpan scheduleInterval)
        {
            this.punctuationType = punctuationType;
            this.scheduleInterval = scheduleInterval;
        }

        public MockProcessor()
            : this(PunctuationType.STREAM_TIME, TimeSpan.MinValue)
        {
        }

        public override void Init(IProcessorContext context)
        {
            base.Init(context);
            if (this.scheduleInterval > TimeSpan.Zero)
            {
                this.scheduleCancellable = context.Schedule(
                    this.scheduleInterval,
                    this.punctuationType,
                    timestamp =>
                    {
                        if (this.punctuationType == PunctuationType.STREAM_TIME)
                        {
                            Assert.Equal(timestamp, context.Timestamp);
                        }

                        Assert.Equal(-1, context.Partition);
                        Assert.Equal(-1L, context.Offset);

                        (this.punctuationType == PunctuationType.STREAM_TIME
                            ? this.punctuatedStreamTime
                            : this.punctuatedSystemTime)
                        .Add(timestamp);
                    });
            }
        }

        public override void Process(K key, V value)
        {
            var keyValueTimestamp = new KeyValueTimestamp<K, V>(key, value, this.Context.Timestamp);

            if (value != null)
            {
                this.LastValueAndTimestampPerKey.Add(key, ValueAndTimestamp.Make(value, this.Context.Timestamp));
            }
            else
            {
                this.LastValueAndTimestampPerKey.Remove(key);
            }

            this.processed.Add(keyValueTimestamp);

            if (this.commitRequested)
            {
                this.Context.Commit();
                this.commitRequested = false;
            }
        }

        public void CheckAndClearProcessResult(params KeyValueTimestamp<K, V>[] expected)
        {
            Assert.Equal(expected.Length, this.processed.Count);
            for (var i = 0; i < expected.Length; i++)
            {
                Assert.Equal(expected[i], this.processed[i]);
            }

            this.processed.Clear();
        }

        public void RequestCommit()
        {
            this.commitRequested = true;
        }

        public void CheckEmptyAndClearProcessResult()
        {
            Assert.Empty(this.processed);
            this.processed.Clear();
        }

        public void CheckAndClearPunctuateResult(PunctuationType type, params DateTime[] expected)
        {
            var punctuated = type == PunctuationType.STREAM_TIME
                ? this.punctuatedStreamTime
                : this.punctuatedSystemTime;

            Assert.Equal(expected.Length, punctuated.Count);

            for (var i = 0; i < expected.Length; i++)
            {
                Assert.Equal(expected[i], punctuated[i]);
            }

            this.processed.Clear();
        }
    }
}
