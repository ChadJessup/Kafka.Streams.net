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
package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.util.Objects;

public class FinalResultsSuppressionBuilder<K : Windowed> implements Suppressed<K>, NamedSuppressed<K> {
    private  string name;
    private  StrictBufferConfig bufferConfig;

    public FinalResultsSuppressionBuilder( string name,  Suppressed.StrictBufferConfig bufferConfig) {
        this.name = name;
        this.bufferConfig = bufferConfig;
    }

    public SuppressedInternal<K> buildFinalResultsSuppression( Duration gracePeriod) {
        return new SuppressedInternal<>(
            name,
            gracePeriod,
            bufferConfig,
            TimeDefinitions.WindowEndTimeDefinition.instance(),
            true
        );
    }

    @Override
    public Suppressed<K> withName( string name) {
        return new FinalResultsSuppressionBuilder<>(name, bufferConfig);
    }

    @Override
    public bool equals( Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
         FinalResultsSuppressionBuilder<?> that = (FinalResultsSuppressionBuilder<?>) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(bufferConfig, that.bufferConfig);
    }

    @Override
    public string name() {
        return name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, bufferConfig);
    }

    @Override
    public string toString() {
        return "FinalResultsSuppressionBuilder{" +
            "name='" + name + '\'' +
            ", bufferConfig=" + bufferConfig +
            '}';
    }
}
