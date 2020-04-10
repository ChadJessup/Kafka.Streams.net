
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
            string Name,
            TimeSpan? suppressionTime,
            IBufferConfig bufferConfig,
            ITimeDefinition<K>? timeDefinition,
            bool safeToDropTombstones)
        {
            this.Name = Name;

            this.timeToWaitForMoreEvents = suppressionTime ?? TimeSpan.FromMilliseconds(long.MaxValue); ;
            this.timeDefinition = timeDefinition ?? RecordTimeDefintion<K>.Instance();
            this.bufferConfig = bufferConfig ?? this.DEFAULT_BUFFER_CONFIG;
            this.safeToDropTombstones = safeToDropTombstones;
        }

        public string Name { get; }

        public ISuppressed<K> WithName(string Name)
        {
            return new SuppressedInternal<K>(
                Name,
                this.timeToWaitForMoreEvents,
                this.bufferConfig,
                this.timeDefinition,
                this.safeToDropTombstones);
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
 
            if (o == null || this.GetType() != o.GetType())
            {
                return false;
            }

            SuppressedInternal<K> that = (SuppressedInternal<K>)o;
            return this.safeToDropTombstones == that.safeToDropTombstones &&
                this.Name.Equals(that.Name) &&
                this.bufferConfig.Equals(that.bufferConfig) &&
                this.timeToWaitForMoreEvents.Equals(that.timeToWaitForMoreEvents) &&
                this.timeDefinition.Equals(that.timeDefinition);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                this.Name,
                this.bufferConfig,
                this.timeToWaitForMoreEvents,
                this.timeDefinition,
                this.safeToDropTombstones);
        }

        public override string ToString()
        {
            return "SuppressedInternal{" +
                    "Name='" + this.Name + '\'' +
                    ", bufferConfig=" + this.bufferConfig +
                    ", timeToWaitForMoreEvents=" + this.timeToWaitForMoreEvents +
                    ", timeDefinition=" + this.timeDefinition +
                    ", safeToDropTombstones=" + this.safeToDropTombstones +
                    '}';
        }
    }
}
