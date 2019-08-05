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
namespace Kafka.Streams.KStream.Internals
{









public KStreamFlatTransformValues<KIn, VIn, VOut> : ProcessorSupplier<KIn, VIn> {

    private  ValueTransformerWithKeySupplier<KIn, VIn, Iterable<VOut>> valueTransformerSupplier;

    public KStreamFlatTransformValues( ValueTransformerWithKeySupplier<KIn, VIn, Iterable<VOut>> valueTransformerWithKeySupplier)
{
        this.valueTransformerSupplier = valueTransformerWithKeySupplier;
    }

    
    public Processor<KIn, VIn> get()
{
        return new KStreamFlatTransformValuesProcessor<>(valueTransformerSupplier());
    }

    public static KStreamFlatTransformValuesProcessor<KIn, VIn, VOut> : Processor<KIn, VIn> {

        private  ValueTransformerWithKey<KIn, VIn, Iterable<VOut>> valueTransformer;
        private IProcessorContext context;

        KStreamFlatTransformValuesProcessor( ValueTransformerWithKey<KIn, VIn, Iterable<VOut>> valueTransformer)
{
            this.valueTransformer = valueTransformer;
        }

        
        public void init( IProcessorContext context)
{
            valueTransformer.init(new ForwardingDisabledProcessorContext(context));
            this.context = context;
        }

        
        public void process( KIn key,  VIn value)
{
             Iterable<VOut> transformedValues = valueTransformer.transform(key, value);
            if (transformedValues != null)
{
                foreach ( VOut transformedValue in transformedValues)
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
