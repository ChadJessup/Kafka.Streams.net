
//using Kafka.Streams.Processors;
//using Microsoft.Extensions.Logging;
//using System;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableKTableJoinProcessor : AbstractProcessor<K, Change<V1>>
//    {

//        private IKTableValueGetter<K, V2> valueGetter;
//        private StreamsMetricsImpl metrics;
//        private Sensor skippedRecordsSensor;

//        KTableKTableJoinProcessor(IKTableValueGetter<K, V2> valueGetter)
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

//            ValueAndTimestamp<V2> valueAndTimestampRight = valueGetter[key];
//            V2 valueRight = getValueOrNull(valueAndTimestampRight);
//            if (valueRight == null)
//            {
//                return;
//            }

//            resultTimestamp = Math.Max(context.timestamp(), valueAndTimestampRight.timestamp());

//            if (change.newValue != null)
//            {
//                newValue = joiner.apply(change.newValue, valueRight);
//            }

//            if (sendOldValues && change.oldValue != null)
//            {
//                oldValue = joiner.apply(change.oldValue, valueRight);
//            }

//            context.forward(key, new Change<>(newValue, oldValue), To.all().withTimestamp(resultTimestamp));
//        }


//        public void close()
//        {
//            valueGetter.close();
//        }
//    }
//}
