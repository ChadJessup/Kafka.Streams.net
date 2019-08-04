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
using System;
using System.Collections.Generic;

namespace Kafka.Common.Metrics.Stats
{
    /**
     * A compound stat that includes a rate metric and a cumulative total metric.
     */
    public Meter : ICompoundStat
    {
        private MetricName rateMetricName;
        private MetricName totalMetricName;
        private Rate rate;
        private CumulativeSum total;

        /**
         * Construct a Meter with seconds as time unit
         */
        public Meter(MetricName rateMetricName, MetricName totalMetricName)
                : this(TimeUnit.SECONDS, new WindowedSum(), rateMetricName, totalMetricName)
        {
        }

        /**
         * Construct a Meter with provided time unit
         */
        public Meter(TimeUnit unit, MetricName rateMetricName, MetricName totalMetricName)
            : this(unit, new WindowedSum(), rateMetricName, totalMetricName)
        {
        }

        /**
         * Construct a Meter with seconds as time unit
         */
        public Meter(SampledStat rateStat, MetricName rateMetricName, MetricName totalMetricName)
            : this(TimeUnit.SECONDS, rateStat, rateMetricName, totalMetricName)
        {
        }

        /**
         * Construct a Meter with provided time unit
         */
        public Meter(TimeUnit unit, SampledStat rateStat, MetricName rateMetricName, MetricName totalMetricName)
        {
            if (!(rateStat is WindowedSum))
            {
                throw new System.ArgumentException("Meter is supported only for WindowedCount or WindowedSum.");
            }
            this.total = new CumulativeSum();
            this.rate = new Rate(unit, rateStat);
            this.rateMetricName = rateMetricName;
            this.totalMetricName = totalMetricName;
        }


        public List<NamedMeasurable> stats()
        {
            return new List<NamedMeasurable>
            {
                new NamedMeasurable(totalMetricName, total),
                new NamedMeasurable(rateMetricName, rate),
            };
        }


        public void record(MetricConfig config, double value, long timeMs)
        {
            rate.record(config, value, timeMs);
            // Total metrics with Count stat should record 1.0 (as recorded in the count)
            double totalValue = (rate.stat is WindowedCount) ? 1.0 : value;
            total.record(config, totalValue, timeMs);
        }
    }
}
