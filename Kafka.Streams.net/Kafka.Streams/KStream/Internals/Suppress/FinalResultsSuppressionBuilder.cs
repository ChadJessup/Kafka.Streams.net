
//using Kafka.Common;
//using Kafka.Streams.KStream.Interfaces;
//using System;

//namespace Kafka.Streams.KStream.Internals.Suppress
//{






//    public class FinalResultsSuppressionBuilder<K> : ISuppressed<K>, INamedSuppressed<K>
//        where K : Windowed<K>
//    {
//        private string name;
//        private IStrictBufferConfig bufferConfig;

//        public FinalResultsSuppressionBuilder(string name, IStrictBufferConfig bufferConfig)
//        {
//            this.name = name;
//            this.bufferConfig = bufferConfig;
//        }

//        public SuppressedInternal<K> buildFinalResultsSuppression(TimeSpan gracePeriod)
//        {
//            return new SuppressedInternal<K>(
//                name,
//                gracePeriod,
//                bufferConfig,
//                ITimeDefinition.WindowEndTimeDefinition.instance(),
//                true
//            );
//        }


//        public ISuppressed<K> withName(string name)
//        {
//            return new FinalResultsSuppressionBuilder<K>(name, bufferConfig);
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
//            return name.Equals(that.name) &&
//                bufferConfig.Equals(that.bufferConfig);
//        }

//        public int hashCode()
//        {
//            return (name, bufferConfig).GetHashCode();
//        }

//        public override string ToString()
//        {
//            return "FinalResultsSuppressionBuilder{" +
//                "name='" + name + '\'' +
//                ", bufferConfig=" + bufferConfig +
//                '}';
//        }
//    }
//}