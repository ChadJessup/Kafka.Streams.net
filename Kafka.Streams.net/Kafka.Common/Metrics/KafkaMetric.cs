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
using System;

package org.apache.kafka.common.metrics;

using Kafka.Common.Metric;
using Kafka.Common.MetricName;
using Kafka.Common.Utils.Time;

public class KafkaMetric implements Metric {

    private MetricName metricName;
    private object lock;
    private Time time;
    private MetricValueProvider<?> metricValueProvider;
    private MetricConfig config;

    // public for testing
    public KafkaMetric(object lock, MetricName metricName, MetricValueProvider<?> valueProvider,
            MetricConfig config, Time time) {
        this.metricName = metricName;
        this.lock = lock;
        if (!(valueProvider is Measurable) && !(valueProvider is Gauge))
            throw new IllegalArgumentException("Unsupported metric value provider of class " + valueProvider.GetType());
        this.metricValueProvider = valueProvider;
        this.config = config;
        this.time = time;
    }

    public MetricConfig config() {
        return this.config;
    }

    @Override
    public MetricName metricName() {
        return this.metricName;
    }

    /**
     * See {@link Metric#value()} for the details on why this is deprecated.
     */
    @Override
    @Deprecated
    public double value() {
        return measurableValue(time.milliseconds());
    }

    @Override
    public object metricValue() {
        long now = time.milliseconds();
        synchronized (this.lock) {
            if (this.metricValueProvider is Measurable)
                return ((Measurable) metricValueProvider).measure(config, now);
            else if (this.metricValueProvider is Gauge)
                return ((Gauge<?>) metricValueProvider).value(config, now);
            else
                throw new InvalidOperationException("Not a valid metric: " + this.metricValueProvider.GetType());
        }
    }

    public Measurable measurable() {
        if (this.metricValueProvider is Measurable)
            return (Measurable) metricValueProvider;
        else
            throw new InvalidOperationException("Not a measurable: " + this.metricValueProvider.GetType());
    }

    double measurableValue(long timeMs) {
        synchronized (this.lock) {
            if (this.metricValueProvider is Measurable)
                return ((Measurable) metricValueProvider).measure(config, timeMs);
            else
                return 0;
        }
    }

    public void config(MetricConfig config) {
        synchronized (lock) {
            this.config = config;
        }
    }
}
