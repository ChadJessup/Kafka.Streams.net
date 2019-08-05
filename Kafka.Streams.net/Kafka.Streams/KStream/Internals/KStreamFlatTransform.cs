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










public KStreamFlatTransform<KIn, VIn, KOut, VOut> : ProcessorSupplier<KIn, VIn> {

    private  TransformerSupplier<KIn, VIn, Iterable<KeyValue<KOut, VOut>>> transformerSupplier;

    public KStreamFlatTransform( TransformerSupplier<KIn, VIn, Iterable<KeyValue<KOut, VOut>>> transformerSupplier)
{
        this.transformerSupplier = transformerSupplier;
    }

    
    public Processor<KIn, VIn> get()
{
        return new KStreamFlatTransformProcessor<>(transformerSupplier());
    }

    public static KStreamFlatTransformProcessor<KIn, VIn, KOut, VOut> : AbstractProcessor<KIn, VIn> {

        private  Transformer<KIn, VIn, Iterable<KeyValue<KOut, VOut>>> transformer;

        public KStreamFlatTransformProcessor( Transformer<KIn, VIn, Iterable<KeyValue<KOut, VOut>>> transformer)
{
            this.transformer = transformer;
        }

        
        public void init( IProcessorContext context)
{
            base.init(context);
            transformer.init(context);
        }

        
        public void process( KIn key,  VIn value)
{
             Iterable<KeyValue<KOut, VOut>> pairs = transformer.transform(key, value);
            if (pairs != null)
{
                foreach ( KeyValue<KOut, VOut> pair in pairs)
{
                    context().forward(pair.key, pair.value);
                }
            }
        }

        
        public void close()
{
            transformer.close();
        }
    }
}
