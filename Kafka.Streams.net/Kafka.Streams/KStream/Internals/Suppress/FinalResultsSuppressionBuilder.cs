using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.KStream.Interfaces;
using System;

namespace Kafka.Streams.KStream.Internals.Suppress
{
    public class FinalResultsSuppressionBuilder<K> : ISuppressed<K>, INamedSuppressed<K>
        where K : IWindowed<K>
    {
        public FinalResultsSuppressionBuilder(string Name, IStrictBufferConfig bufferConfig)
        {
            this.Name = Name;
            this.bufferConfig = bufferConfig;
        }

        public FinalResultsSuppressionBuilder(string name, IBufferConfig bufferConfig)
        {
            this.Name = name;
            this.bufferConfig = bufferConfig;
        }

        public string Name { get; }
        public ITimeDefinition<K> timeDefinition { get; }
        public IBufferConfig bufferConfig { get; }
        public bool safeToDropTombstones { get; }

        public SuppressedInternal<K> buildFinalResultsSuppression(TimeSpan gracePeriod)
        {
            return new SuppressedInternal<K>(
                this.Name,
                gracePeriod,
                this.bufferConfig,
                null,//ITimeDefinition<K>.WindowEndTimeDefinition.instance(),
                true
            );
        }


        public ISuppressed<K> WithName(string Name)
        {
            return new FinalResultsSuppressionBuilder<K>(Name, this.bufferConfig);
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

            FinalResultsSuppressionBuilder<K> that = (FinalResultsSuppressionBuilder<K>)o;
            return this.Name.Equals(that.Name) &&
                this.bufferConfig.Equals(that.bufferConfig);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(this.Name, this.bufferConfig);
        }

        public override string ToString()
        {
            return "FinalResultsSuppressionBuilder{" +
                "Name='" + this.Name + '\'' +
                ", bufferConfig=" + this.bufferConfig +
                '}';
        }

        public TimeSpan TimeToWaitForMoreEvents()
        {
            throw new NotImplementedException();
        }
    }
}
