/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

public  class FullChangeSerde<T> {
    private  ISerde<T> inner;

    public static <T> FullChangeSerde<T> wrap( ISerde<T> serde) {
        if (serde == null) {
            return null;
        } else {
            return new FullChangeSerde<>(serde);
        }
    }

    private FullChangeSerde( ISerde<T> inner) {
        this.inner = requireNonNull(inner);
    }

    public ISerde<T> innerSerde() {
        return inner;
    }

    public Change<byte[]> serializeParts( string topic,  Change<T> data) {
        if (data == null) {
            return null;
        }
         Serializer<T> innerSerializer = innerSerde().serializer();
         byte[] oldBytes = data.oldValue == null ? null : innerSerializer.serialize(topic, data.oldValue);
         byte[] newBytes = data.newValue == null ? null : innerSerializer.serialize(topic, data.newValue);
        return new Change<>(newBytes, oldBytes);
    }


    public Change<T> deserializeParts( string topic,  Change<byte[]> serialChange) {
        if (serialChange == null) {
            return null;
        }
         Deserializer<T> innerDeserializer = innerSerde().deserializer();

         T oldValue =
            serialChange.oldValue == null ? null : innerDeserializer.deserialize(topic, serialChange.oldValue);
         T newValue =
            serialChange.newValue == null ? null : innerDeserializer.deserialize(topic, serialChange.newValue);

        return new Change<>(newValue, oldValue);
    }

    /**
     * We used to serialize a Change into a single byte[]. Now, we don't anymore, but we still keep this logic here
     * so that we can produce the legacy format to test that we can still deserialize it.
     */
    public static byte[] mergeChangeArraysIntoSingleLegacyFormattedArray( Change<byte[]> serialChange) {
        if (serialChange == null) {
            return null;
        }

         int oldSize = serialChange.oldValue == null ? -1 : serialChange.oldValue.length;
         int newSize = serialChange.newValue == null ? -1 : serialChange.newValue.length;

         ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES * 2 + Math.max(0, oldSize) + Math.max(0, newSize));


        buffer.putInt(oldSize);
        if (serialChange.oldValue != null) {
            buffer.put(serialChange.oldValue);
        }

        buffer.putInt(newSize);
        if (serialChange.newValue != null) {
            buffer.put(serialChange.newValue);
        }
        return buffer.array();
    }

    /**
     * We used to serialize a Change into a single byte[]. Now, we don't anymore, but we still
     * need to be able to read it (so that we can load the state store from previously-written changelog records).
     */
    public static Change<byte[]> decomposeLegacyFormattedArrayIntoChangeArrays( byte[] data) {
        if (data == null) {
            return null;
        }
         ByteBuffer buffer = ByteBuffer.wrap(data);

         int oldSize = buffer.getInt();
         byte[] oldBytes = oldSize == -1 ? null : new byte[oldSize];
        if (oldBytes != null) {
            buffer.get(oldBytes);
        }

         int newSize = buffer.getInt();
         byte[] newBytes = newSize == -1 ? null : new byte[newSize];
        if (newBytes != null) {
            buffer.get(newBytes);
        }

        return new Change<>(newBytes, oldBytes);
    }

}
