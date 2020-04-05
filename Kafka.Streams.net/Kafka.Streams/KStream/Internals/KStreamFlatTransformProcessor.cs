
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Interfaces;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KStreamFlatTransformProcessor<KIn, VIn, KOut, VOut> : AbstractProcessor<KIn, VIn>
//    {
//        private ITransformer<KIn, VIn, IEnumerable<KeyValuePair<KOut, VOut>>> transformer;

//        public KStreamFlatTransformProcessor(ITransformer<KIn, VIn, IEnumerable<KeyValuePair<KOut, VOut>>> transformer)
//        {
//            this.transformer = transformer;
//        }


//        public void init(IProcessorContext context)
//        {
//            base.Init(context);
//            transformer.Init(context);
//        }


//        public override void process(KIn key, VIn value)
//        {
//            IEnumerable<KeyValuePair<KOut, VOut>> pairs = transformer.transform(key, value);
//            if (pairs != null)
//            {
//                foreach (KeyValuePair<KOut, VOut> pair in pairs)
//                {
//                    //context.Forward(pair.key, pair.value);
//                }
//            }
//        }


//        public override void close()
//        {
//            transformer.close();
//        }
//    }
//}
