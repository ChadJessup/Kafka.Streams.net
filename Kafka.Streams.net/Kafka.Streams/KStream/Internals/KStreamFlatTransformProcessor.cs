﻿
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Interfaces;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KStreamFlatTransformProcessor<KIn, VIn, KOut, VOut> : AbstractProcessor<KIn, VIn>
//    {
//        private ITransformer<KIn, VIn, IEnumerable<KeyValue<KOut, VOut>>> transformer;

//        public KStreamFlatTransformProcessor(ITransformer<KIn, VIn, IEnumerable<KeyValue<KOut, VOut>>> transformer)
//        {
//            this.transformer = transformer;
//        }


//        public void init(IProcessorContext context)
//        {
//            base.init(context);
//            transformer.init(context);
//        }


//        public override void process(KIn key, VIn value)
//        {
//            IEnumerable<KeyValue<KOut, VOut>> pairs = transformer.transform(key, value);
//            if (pairs != null)
//            {
//                foreach (KeyValue<KOut, VOut> pair in pairs)
//                {
//                    //context.forward(pair.key, pair.value);
//                }
//            }
//        }


//        public override void close()
//        {
//            transformer.close();
//        }
//    }
//}
