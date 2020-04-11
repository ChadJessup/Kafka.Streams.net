using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Mocks
{
    public class MockProcessor<K, V> : AbstractProcessor<K, V>
    {
        public List<KeyValueTimestamp<K, V>> processed = new List<KeyValueTimestamp<K, V>>();
        public Dictionary<K, IValueAndTimestamp<V>> lastValueAndTimestampPerKey = new Dictionary<K, IValueAndTimestamp<V>>();

        public List<long> punctuatedStreamTime = new List<long>();
        public List<long> punctuatedSystemTime = new List<long>();


        private readonly PunctuationType punctuationType;
        private readonly long scheduleInterval;

        private bool commitRequested = false;
        private ICancellable scheduleCancellable;

        public MockProcessor(
            PunctuationType punctuationType,
            long scheduleInterval)
        {
            this.punctuationType = punctuationType;
            this.scheduleInterval = scheduleInterval;
        }

        public MockProcessor()
            : this(PunctuationType.STREAM_TIME, -1)
        {
        }

        public override void Init(IProcessorContext context)
        {
            base.Init(context);
            if (this.scheduleInterval > 0L)
            {
                this.scheduleCancellable = context.Schedule(
                    TimeSpan.FromMilliseconds(this.scheduleInterval),
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
                this.lastValueAndTimestampPerKey.Add(key, ValueAndTimestamp.Make(value, this.Context.Timestamp));
            }
            else
            {
                this.lastValueAndTimestampPerKey.Remove(key);
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

        public void CheckAndClearPunctuateResult(PunctuationType type, params long[] expected)
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
