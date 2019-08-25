/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public partial class KStreamFlatTransformValues<KIn, VIn, VOut>
    {
        public class KStreamFlatTransformValuesProcessor<KIn, VIn, VOut> : IProcessor<KIn, VIn>
        {
            private IValueTransformerWithKey<KIn, VIn, IEnumerable<VOut>> valueTransformer;
            private IProcessorContext<KIn, VOut> context;

            public KStreamFlatTransformValuesProcessor(IValueTransformerWithKey<KIn, VIn, IEnumerable<VOut>> valueTransformer)
            {
                this.valueTransformer = valueTransformer;
            }

            public void init(IProcessorContext<KIn, VIn> context)
            {
                valueTransformer.init(new ForwardingDisabledProcessorContext<KIn, VIn>(context));
                this.context = (IProcessorContext<KIn, VOut>)context;
            }

            public void process(KIn key, VIn value)
            {
                IEnumerable<VOut> transformedValues = valueTransformer.transform(key, value);
                if (transformedValues != null)
                {
                    foreach (VOut transformedValue in transformedValues)
                    {
                        context.forward(key, transformedValue);
                    }
                }
            }

            public void close()
            {
                valueTransformer.close();
            }
        }
    }
}
