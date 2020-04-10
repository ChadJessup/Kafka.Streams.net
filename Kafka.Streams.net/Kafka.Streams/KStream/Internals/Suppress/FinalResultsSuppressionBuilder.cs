
//using Kafka.Common;
//using Kafka.Streams.KStream.Interfaces;
//using System;

//namespace Kafka.Streams.KStream.Internals.Suppress
//{






//    public class FinalResultsSuppressionBuilder<K> : ISuppressed<K>, INamedSuppressed<K>
//        where K : IWindowed<K>
//    {
//        private string Name;
//        private IStrictBufferConfig bufferConfig;

//        public FinalResultsSuppressionBuilder(string Name, IStrictBufferConfig bufferConfig)
//        {
//            this.Name = Name;
//            this.bufferConfig = bufferConfig;
//        }

//        public SuppressedInternal<K> buildFinalResultsSuppression(TimeSpan gracePeriod)
//        {
//            return new SuppressedInternal<K>(
//                Name,
//                gracePeriod,
//                bufferConfig,
//                ITimeDefinition.WindowEndTimeDefinition.instance(),
//                true
//            );
//        }


//        public ISuppressed<K> withName(string Name)
//        {
//            return new FinalResultsSuppressionBuilder<K>(Name, bufferConfig);
//        }


//        public bool Equals(object o)
//        {
//            if (this == o)
//            {
//                return true;
//            }
//            if (o == null || GetType() != o.GetType())
//            {
//                return false;
//            }

//            FinalResultsSuppressionBuilder<K> that = (FinalResultsSuppressionBuilder<K>) o;
//            return Name.Equals(that.Name) &&
//                bufferConfig.Equals(that.bufferConfig);
//        }

//        public int hashCode()
//        {
//            return (Name, bufferConfig).GetHashCode();
//        }

//        public override string ToString()
//        {
//            return "FinalResultsSuppressionBuilder{" +
//                "Name='" + Name + '\'' +
//                ", bufferConfig=" + bufferConfig +
//                '}';
//        }
//    }
//}