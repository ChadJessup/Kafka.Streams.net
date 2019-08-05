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
namespace Kafka.Streams.Processor.Internals;

using Kafka.Common.serialization.Serializer;





public class SinkNode<K, V> : ProcessorNode<K, V> {

    private Serializer<K> keySerializer;
    private Serializer<V> valSerializer;
    private TopicNameExtractor<K, V> topicExtractor;
    private StreamPartitioner<K, V> partitioner;

    private InternalProcessorContext context;

    SinkNode(string name,
             TopicNameExtractor<K, V> topicExtractor,
             Serializer<K> keySerializer,
             Serializer<V> valSerializer,
             StreamPartitioner<K, V> partitioner)
{
        base(name);

        this.topicExtractor = topicExtractor;
        this.keySerializer = keySerializer;
        this.valSerializer = valSerializer;
        this.partitioner = partitioner;
    }

    /**
     * @throws InvalidOperationException if this method.Adds a child to a sink node
     */

    public void addChild(ProcessorNode<?, ?> child)
{
        throw new InvalidOperationException("sink node does not allow.AddChild");
    }



    public void init(InternalProcessorContext context)
{
        base.init(context);
        this.context = context;

        // if serializers are null, get the default ones from the context
        if (keySerializer == null)
{
            keySerializer = (Serializer<K>) context.keySerde().serializer();
        }
        if (valSerializer == null)
{
            valSerializer = (Serializer<V>) context.valueSerde().serializer();
        }

        // if value serializers are for {@code Change} values, set the inner serializer when necessary
        if (valSerializer is ChangedSerializer &&
                ((ChangedSerializer) valSerializer).inner() == null)
{
            ((ChangedSerializer) valSerializer).setInner(context.valueSerde().serializer());
        }
    }



    public void process(K key, V value)
{
        RecordCollector collector = ((RecordCollector.Supplier) context).recordCollector();

        long timestamp = context.timestamp();
        if (timestamp < 0)
{
            throw new StreamsException("Invalid (negative) timestamp of " + timestamp + " for output record <" + key + ":" + value + ">.");
        }

        string topic = topicExtractor.extract(key, value, this.context.recordContext());

        try
{

            collector.send(topic, key, value, context.headers(), timestamp, keySerializer, valSerializer, partitioner);
        } catch (ClassCastException e)
{
            string keyClass = key == null ? "unknown because key is null" : key.GetType().getName();
            string valueClass = value == null ? "unknown because value is null" : value.GetType().getName();
            throw new StreamsException(
                    string.Format("A serializer (key: %s / value: %s) is not compatible to the actual key or value type " +
                                    "(key type: %s / value type: %s). Change the default Serdes in StreamConfig or " +
                                    "provide correct Serdes via method parameters.",
                                    keySerializer.GetType().getName(),
                                    valSerializer.GetType().getName(),
                                    keyClass,
                                    valueClass),
                    e);
        }
    }

    /**
     * @return a string representation of this node, useful for debugging.
     */

    public string ToString()
{
        return ToString("");
    }

    /**
     * @return a string representation of this node starting with the given indent, useful for debugging.
     */

    public string ToString(string indent)
{
        StringBuilder sb = new StringBuilder(base.ToString(indent));
        sb.Append(indent).Append("\ttopic:\t\t");
        sb.Append(topicExtractor);
        sb.Append("\n");
        return sb.ToString();
    }

}
