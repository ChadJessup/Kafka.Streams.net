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
namespace Kafka.streams.kstream.internals;








public class TransformerSupplierAdapter<KIn, VIn, KOut, VOut> : TransformerSupplier<KIn, VIn, Iterable<KeyValue<KOut, VOut>>> {

    private TransformerSupplier<KIn, VIn, KeyValue<KOut, VOut>> transformerSupplier;

    public TransformerSupplierAdapter( TransformerSupplier<KIn, VIn, KeyValue<KOut, VOut>> transformerSupplier)
{
        this.transformerSupplier = transformerSupplier;
    }

    
    public Transformer<KIn, VIn, Iterable<KeyValue<KOut, VOut>>> get()
{
        return new Transformer<KIn, VIn, Iterable<KeyValue<KOut, VOut>>>()
{

            private Transformer<KIn, VIn, KeyValue<KOut, VOut>> transformer = transformerSupplier[];

            
            public void init( IProcessorContext context)
{
                transformer.init(context);
            }

            
            public Iterable<KeyValue<KOut, VOut>> transform( KIn key,  VIn value)
{
                 KeyValue<KOut, VOut> pair = transformer.transform(key, value);
                if (pair != null)
{
                    return Collections.singletonList(pair);
                }
                return Collections.emptyList();
            }

            
            public void close()
{
                transformer.close();
            }
        };
    }
}
