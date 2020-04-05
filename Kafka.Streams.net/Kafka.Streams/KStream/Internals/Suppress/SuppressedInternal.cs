
using Kafka.Streams.KStream.Interfaces;
using System;

namespace Kafka.Streams.KStream.Internals.Suppress
{
    public class SuppressedInternal<K> : ISuppressed<K>
    {
        private readonly IStrictBufferConfig DEFAULT_BUFFER_CONFIG = IBufferConfig.Unbounded();
        private readonly TimeSpan? timeToWaitForMoreEvents;

        internal readonly ITimeDefinition<K> timeDefinition;
        internal readonly IBufferConfig bufferConfig;
        internal readonly bool safeToDropTombstones;

        public SuppressedInternal(
            string name,
            TimeSpan? suppressionTime,
            IBufferConfig bufferConfig,
            ITimeDefinition<K>? timeDefinition,
            bool safeToDropTombstones)
        {
            this.Name = name;

            this.timeToWaitForMoreEvents = suppressionTime ?? TimeSpan.FromMilliseconds(long.MaxValue); ;
            this.timeDefinition = timeDefinition ?? RecordTimeDefintion<K>.Instance();
            this.bufferConfig = bufferConfig ?? DEFAULT_BUFFER_CONFIG;
            this.safeToDropTombstones = safeToDropTombstones;
        }

        public string Name { get; }

        public ISuppressed<K> WithName(string name)
        {
            return new SuppressedInternal<K>(
                name,
                timeToWaitForMoreEvents,
                bufferConfig,
                timeDefinition,
                safeToDropTombstones);
        }

        public TimeSpan TimeToWaitForMoreEvents()
        {
            return this.timeToWaitForMoreEvents ?? TimeSpan.Zero;
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
 
            if (o == null || GetType() != o.GetType())
            {
                return false;
            }

            SuppressedInternal<K> that = (SuppressedInternal<K>)o;
            return safeToDropTombstones == that.safeToDropTombstones &&
                Name.Equals(that.Name) &&
                bufferConfig.Equals(that.bufferConfig) &&
                timeToWaitForMoreEvents.Equals(that.timeToWaitForMoreEvents) &&
                timeDefinition.Equals(that.timeDefinition);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                Name,
                bufferConfig,
                timeToWaitForMoreEvents,
                timeDefinition,
                safeToDropTombstones);
        }

        public override string ToString()
        {
            return "SuppressedInternal{" +
                    "name='" + Name + '\'' +
                    ", bufferConfig=" + bufferConfig +
                    ", timeToWaitForMoreEvents=" + timeToWaitForMoreEvents +
                    ", timeDefinition=" + timeDefinition +
                    ", safeToDropTombstones=" + safeToDropTombstones +
                    '}';
        }
    }
}
