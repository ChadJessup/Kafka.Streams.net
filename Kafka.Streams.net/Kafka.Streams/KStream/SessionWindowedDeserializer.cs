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
namespace Kafka.Streams.KStream
{











/**
 *  The inner serde can be specified by setting the property
 *  {@link StreamsConfig#DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS} or
 *  {@link StreamsConfig#DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS}
 *  if the no-arg constructor is called and hence it is not passed during initialization.
 */
public SessionWindowedDeserializer<T> : Deserializer<Windowed<T>> {

    private Deserializer<T> inner;

    // Default constructor needed by Kafka
    public SessionWindowedDeserializer() {}

    public SessionWindowedDeserializer( Deserializer<T> inner)
{
        this.inner = inner;
    }

    
    
    public void configure( Map<string, ?> configs,  bool isKey)
{
        if (inner == null)
{
             string propertyName = isKey ? StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS : StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS;
             string value = (string) configs[propertyName);
            try
{

                inner = Serde.cast(Utils.newInstance(value, Serde)).deserializer();
                inner.configure(configs, isKey);
            } catch ( ClassNotFoundException e)
{
                throw new ConfigException(propertyName, value, "Serde " + value + " could not be found.");
            }
        }
    }

    
    public Windowed<T> deserialize( string topic,  byte[] data)
{
        WindowedSerdes.verifyInnerDeserializerNotNull(inner, this);

        if (data == null || data.Length == 0)
{
            return null;
        }

        // for either key or value, their schema is the same hence we will just use session key schema
        return SessionKeySchema.from(data, inner, topic);
    }

    
    public void close()
{
        if (inner != null)
{
            inner.close();
        }
    }

    // Only for testing
    Deserializer<T> innerDeserializer()
{
        return inner;
    }
}
