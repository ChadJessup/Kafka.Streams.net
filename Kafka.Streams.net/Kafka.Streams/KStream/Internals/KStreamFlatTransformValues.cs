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
using Kafka.Streams.Processors;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public partial class KStreamFlatTransformValues<KIn, VIn, VOut> : IProcessorSupplier<KIn, VIn>
    {
        private readonly IValueTransformerWithKeySupplier<KIn, VIn, IEnumerable<VOut>> valueTransformerSupplier;

        public KStreamFlatTransformValues(IValueTransformerWithKeySupplier<KIn, VIn, IEnumerable<VOut>> valueTransformerWithKeySupplier)
        {
            this.valueTransformerSupplier = valueTransformerWithKeySupplier;
        }

        public IKeyValueProcessor<KIn, VIn> Get()
        {
            return new KStreamFlatTransformValuesProcessor<KIn, VIn, VOut>(this.valueTransformerSupplier.get());
        }
    }
}
