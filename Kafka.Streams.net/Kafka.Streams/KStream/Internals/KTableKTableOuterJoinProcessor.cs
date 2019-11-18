
//using Kafka.Streams.Processors;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableKTableOuterJoinProcessor : AbstractProcessor<K, Change<V1>>
//    {

//        private IKTableValueGetter<K, V2> valueGetter;
//        private StreamsMetricsImpl metrics;
//        private Sensor skippedRecordsSensor;

//        KTableKTableOuterJoinProcessor(IKTableValueGetter<K, V2> valueGetter)
//        {
//            this.valueGetter = valueGetter;
//        }


//        public void init(IProcessorContext context)
//        {
//            base.init(context);
//            metrics = (StreamsMetricsImpl)context.metrics;
//            skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);
//            valueGetter.init(context);
//        }


//        public void process(K key, Change<V1> change)
//        {
//            // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
//            if (key == null)
//            {
//                LOG.LogWarning(
//                    "Skipping record due to null key. change=[{}] topic=[{}] partition=[{}] offset=[{}]",
//                    change, context.Topic, context.partition(), context.offset()
//                );
//                skippedRecordsSensor.record();
//                return;
//            }

//            R newValue = null;
//            long resultTimestamp;
//            R oldValue = null;

//            ValueAndTimestamp<V2> valueAndTimestamp2 = valueGetter[key];
//            V2 value2 = getValueOrNull(valueAndTimestamp2);
//            if (value2 == null)
//            {
//                if (change.newValue == null && change.oldValue == null)
//                {
//                    return;
//                }
//                resultTimestamp = context.timestamp();
//            }
//            else
//            {

//                resultTimestamp = Math.Max(context.timestamp(), valueAndTimestamp2.timestamp());
//            }

//            if (value2 != null || change.newValue != null)
//            {
//                newValue = joiner.apply(change.newValue, value2);
//            }

//            if (sendOldValues && (value2 != null || change.oldValue != null))
//            {
//                oldValue = joiner.apply(change.oldValue, value2);
//            }

//            context.forward(key, new Change<>(newValue, oldValue), To.all().withTimestamp(resultTimestamp));
//        }


//        public void close()
//        {
//            valueGetter.close();
//        }
//    }
//}
