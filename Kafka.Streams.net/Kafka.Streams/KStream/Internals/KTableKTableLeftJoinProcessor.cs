﻿
//using Microsoft.Extensions.Logging;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableKTableLeftJoinProcessor : AbstractProcessor<K, Change<V1>>
//    {

//        private IKTableValueGetter<K, V2> valueGetter;
//        private StreamsMetricsImpl metrics;
//        private Sensor skippedRecordsSensor;

//        KTableKTableLeftJoinProcessor(IKTableValueGetter<K, V2> valueGetter)
//        {
//            this.valueGetter = valueGetter;
//        }


//        public void Init(IProcessorContext context)
//        {
//            base.Init(context);
//            metrics = (StreamsMetricsImpl)context.metrics;
//            skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);
//            valueGetter.Init(context);
//        }


//        public void process(K key, Change<V1> change)
//        {
//            // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
//            if (key == null)
//            {
//                LOG.LogWarning(
//                    "Skipping record due to null key. change=[{}] topic=[{}] partition=[{}] offset=[{}]",
//                    change, context.Topic, context.Partition, context.offset()
//                );
//                skippedRecordsSensor.record();
//                return;
//            }

//            R newValue = null;
//            long resultTimestamp;
//            R oldValue = null;

//            ValueAndTimestamp<V2> valueAndTimestampRight = valueGetter[key];
//            V2 value2 = ValueAndTimestamp.GetValueOrNull(valueAndTimestampRight);
//            long timestampRight;

//            if (value2 == null)
//            {
//                if (change.newValue == null && change.oldValue == null)
//                {
//                    return;
//                }
//                timestampRight = UNKNOWN;
//            }
//            else
//            {

//                timestampRight = valueAndTimestampRight.timestamp();
//            }

//            resultTimestamp = Math.Max(context.timestamp(), timestampRight);

//            if (change.newValue != null)
//            {
//                newValue = joiner.apply(change.newValue, value2);
//            }

//            if (sendOldValues && change.oldValue != null)
//            {
//                oldValue = joiner.apply(change.oldValue, value2);
//            }

//            context.Forward(key, new Change<>(newValue, oldValue), To.All().WithTimestamp(resultTimestamp));
//        }


//        public void Close()
//        {
//            valueGetter.Close();
//        }
//    }
//}
