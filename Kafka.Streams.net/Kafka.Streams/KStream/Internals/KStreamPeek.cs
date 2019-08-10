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







class KStreamPeek<K, V> : IProcessorSupplier<K, V> {

    private  bool forwardDownStream;
    private  ForeachAction<K, V> action;

    public KStreamPeek( ForeachAction<K, V> action,  bool forwardDownStream)
{
        this.action = action;
        this.forwardDownStream = forwardDownStream;
    }

    
    public IProcessor<K, V> get()
{
        return new KStreamPeekProcessor();
    }

    private KStreamPeekProcessor : AbstractProcessor<K, V> {
        
        public void process( K key,  V value)
{
            action.apply(key, value);
            if (forwardDownStream)
{
                context.forward(key, value);
            }
        }
    }

}
