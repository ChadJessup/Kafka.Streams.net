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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.errors.TopologyException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * An object to define the options used when printing a {@link KStream}.
 *
 * @param <K> key type
 * @param <V> value type
 * @see KStream#print(Printed)
 */
public class Printed<K, V> implements NamedOperation<Printed<K, V>> {
    protected  OutputStream outputStream;
    protected string label;
    protected string processorName;
    protected KeyValueMapper<? super K, ? super V, string> mapper = new KeyValueMapper<K, V, string>() {
        @Override
        public string apply( K key,  V value) {
            return string.format("%s, %s", key, value);
        }
    };

    private Printed( OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    /**
     * Copy constructor.
     * @param printed   instance of {@link Printed} to copy
     */
    protected Printed( Printed<K, V> printed) {
        this.outputStream = printed.outputStream;
        this.label = printed.label;
        this.mapper = printed.mapper;
        this.processorName = printed.processorName;
    }

    /**
     * Print the records of a {@link KStream} to a file.
     *
     * @param filePath path of the file
     * @param <K>      key type
     * @param <V>      value type
     * @return a new Printed instance
     */
    public static <K, V> Printed<K, V> toFile( string filePath) {
        Objects.requireNonNull(filePath, "filePath can't be null");
        if (filePath.trim().isEmpty()) {
            throw new TopologyException("filePath can't be an empty string");
        }
        try {
            return new Printed<>(Files.newOutputStream(Paths.get(filePath)));
        } catch ( IOException e) {
            throw new TopologyException("Unable to write stream to file at [" + filePath + "] " + e.getMessage());
        }
    }

    /**
     * Print the records of a {@link KStream} to system out.
     *
     * @param <K> key type
     * @param <V> value type
     * @return a new Printed instance
     */
    public static <K, V> Printed<K, V> toSysOut() {
        return new Printed<>(System.out);
    }

    /**
     * Print the records of a {@link KStream} with the provided label.
     *
     * @param label label to use
     * @return this
     */
    public Printed<K, V> withLabel( string label) {
        Objects.requireNonNull(label, "label can't be null");
        this.label = label;
        return this;
    }

    /**
     * Print the records of a {@link KStream} with the provided {@link KeyValueMapper}
     * The provided KeyValueMapper's mapped value type must be {@code string}.
     * <p>
     * The example below shows how to customize output data.
     * <pre>{@code
     *  KeyValueMapper<Integer, string, string> mapper = new KeyValueMapper<Integer, string, string>() {
     *     public string apply(Integer key, string value) {
     *         return string.format("(%d, %s)", key, value);
     *     }
     * };
     * }</pre>
     *
     * Implementors will need to override {@code toString()} for keys and values that are not of type {@link string},
     * {@link Integer} etc. to get meaningful information.
     *
     * @param mapper mapper to use
     * @return this
     */
    public Printed<K, V> withKeyValueMapper( KeyValueMapper<? super K, ? super V, string> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        this.mapper = mapper;
        return this;
    }

    /**
     * Print the records of a {@link KStream} with provided processor name.
     *
     * @param processorName the processor name to be used. If {@code null} a default processor name will be generated
     ** @return this
     */
    @Override
    public Printed<K, V> withName( string processorName) {
        this.processorName = processorName;
        return this;
    }
}
