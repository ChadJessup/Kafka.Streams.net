﻿using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using NodaTime;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Mocks
{
    public class MockProcessor<K, V> : AbstractProcessor<K, V>
    {
        public List<KeyValueTimestamp<K, V>> processed = new List<KeyValueTimestamp<K, V>>();
        public Dictionary<K, ValueAndTimestamp<V>> lastValueAndTimestampPerKey = new Dictionary<K, ValueAndTimestamp<V>>();

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
            if (scheduleInterval > 0L)
            {
                scheduleCancellable = context.Schedule(
                    Duration.FromMilliseconds(scheduleInterval).ToTimeSpan(),
                    punctuationType,
                    timestamp =>
                    {
                        if (punctuationType == PunctuationType.STREAM_TIME)
                        {
                            Assert.Equal(timestamp, context.Timestamp);
                        }

                        Assert.Equal(-1, context.Partition);
                        Assert.Equal(-1L, context.Offset);

                        (punctuationType == PunctuationType.STREAM_TIME
                            ? punctuatedStreamTime
                            : punctuatedSystemTime)
                        .Add(timestamp);
                    });
            }
        }

        public override void Process(K key, V value)
        {
            var keyValueTimestamp = new KeyValueTimestamp<K, V>(key, value, Context.Timestamp);

            if (value != null)
            {
                lastValueAndTimestampPerKey.Add(key, ValueAndTimestamp.Make(value, Context.Timestamp));
            }
            else
            {
                lastValueAndTimestampPerKey.Remove(key);
            }

            processed.Add(keyValueTimestamp);

            if (commitRequested)
            {
                this.Context.Commit();
                commitRequested = false;
            }
        }

        public void CheckAndClearProcessResult(params KeyValueTimestamp<K, V>[] expected)
        {
            Assert.Equal(expected.Length, processed.Count);
            for (var i = 0; i < expected.Length; i++)
            {
                Assert.Equal(expected[i], processed[i]);
            }

            processed.Clear();
        }

        public void RequestCommit()
        {
            commitRequested = true;
        }

        public void CheckEmptyAndClearProcessResult()
        {
            Assert.Empty(processed);
            processed.Clear();
        }

        public void CheckAndClearPunctuateResult(PunctuationType type, params long[] expected)
        {
            var punctuated = type == PunctuationType.STREAM_TIME
                ? punctuatedStreamTime
                : punctuatedSystemTime;

            Assert.Equal(expected.Length, punctuated.Count);

            for (var i = 0; i < expected.Length; i++)
            {
                Assert.Equal(expected[i], punctuated[i]);
            }

            processed.Clear();
        }
    }
}
