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
using Confluent.Kafka;

namespace Kafka.Streams.KStream.Internals
{








public class ChangedSerializer<T> : ISerializer<Change<T>> {

    private static  int NEWFLAG_SIZE = 1;

    private ISerializer<T> inner;

    public ChangedSerializer( ISerializer<T> inner)
{
        this.inner = inner;
    }

    public ISerializer<T> inner()
{
        return inner;
    }

    public void setInner( ISerializer<T> inner)
{
        this.inner = inner;
    }

    /**
     * @throws StreamsException if both old and new values of data are null, or if
     * both values are not null
     */

    public byte[] serialize( string topic,  Headers headers,  Change<T> data)
{
         byte[] serializedKey;

        // only one of the old / new values would be not null
        if (data.newValue != null)
{
            if (data.oldValue != null)
{
                throw new StreamsException("Both old and new values are not null (" + data.oldValue
                    + " : " + data.newValue + ") in ChangeSerializer, which is not allowed.");
            }

            serializedKey = inner.serialize(topic, headers, data.newValue);
        } else
{

            if (data.oldValue == null)
{
                throw new StreamsException("Both old and new values are null in ChangeSerializer, which is not allowed.");
            }

            serializedKey = inner.serialize(topic, headers, data.oldValue);
        }

         ByteBuffer buf = ByteBuffer.allocate(serializedKey.Length + NEWFLAG_SIZE);
        buf.Add(serializedKey);
        buf.Add((byte) (data.newValue != null ? 1 : 0));

        return buf.array();
    }


    public byte[] serialize( string topic,  Change<T> data)
{
        return serialize(topic, null, data);
    }


    public void close()
{
        inner.close();
    }
}
