///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.kafka.common.metrics;

//import java.lang.management.ManagementFactory;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;

//import javax.management.Attribute;
//import javax.management.AttributeList;
//import javax.management.AttributeNotFoundException;
//import javax.management.DynamicMBean;
//import javax.management.InvalidAttributeValueException;
//import javax.management.JMException;
//import javax.management.MBeanAttributeInfo;
//import javax.management.MBeanException;
//import javax.management.MBeanInfo;
//import javax.management.MBeanServer;
//import javax.management.MalformedObjectNameException;
//import javax.management.ObjectName;
//import javax.management.ReflectionException;

//using Kafka.Common.KafkaException;
//using Kafka.Common.MetricName;
//using Kafka.Common.Utils.Sanitizer;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

///**
// * Register metrics in JMX as dynamic mbeans based on the metric names
// */
//public class JmxReporter implements MetricsReporter {

//    private static Logger log = LoggerFactory.getLogger(JmxReporter.class);
//    private static object LOCK = new Object();
//    private string prefix;
//    private Dictionary<string, KafkaMbean> mbeans = new HashMap<string, KafkaMbean>();

//    public JmxReporter() {
//        this("");
//    }

//    /**
//     * Create a JMX reporter that prefixes all metrics with the given string.
//     */
//    public JmxReporter(string prefix) {
//        this.prefix = prefix;
//    }

//    @Override
//    public void configure(Dictionary<string, ?> configs) {}

//    @Override
//    public void init(List<KafkaMetric> metrics) {
//        synchronized (LOCK) {
//            for (KafkaMetric metric : metrics)
//                addAttribute(metric);
//            for (KafkaMbean mbean : mbeans.values())
//                reregister(mbean);
//        }
//    }

//    public bool containsMbean(string mbeanName) {
//        return mbeans.containsKey(mbeanName);
//    }
//    @Override
//    public void metricChange(KafkaMetric metric) {
//        synchronized (LOCK) {
//            KafkaMbean mbean = addAttribute(metric);
//            reregister(mbean);
//        }
//    }

//    @Override
//    public void metricRemoval(KafkaMetric metric) {
//        synchronized (LOCK) {
//            MetricName metricName = metric.metricName();
//            string mBeanName = getMBeanName(prefix, metricName);
//            KafkaMbean mbean = removeAttribute(metric, mBeanName);
//            if (mbean != null) {
//                if (mbean.metrics.isEmpty()) {
//                    unregister(mbean);
//                    mbeans.remove(mBeanName);
//                } else
//                    reregister(mbean);
//            }
//        }
//    }

//    private KafkaMbean removeAttribute(KafkaMetric metric, string mBeanName) {
//        MetricName metricName = metric.metricName();
//        KafkaMbean mbean = this.mbeans.get(mBeanName);
//        if (mbean != null)
//            mbean.removeAttribute(metricName.name());
//        return mbean;
//    }

//    private KafkaMbean addAttribute(KafkaMetric metric) {
//        try {
//            MetricName metricName = metric.metricName();
//            string mBeanName = getMBeanName(prefix, metricName);
//            if (!this.mbeans.containsKey(mBeanName))
//                mbeans.put(mBeanName, new KafkaMbean(mBeanName));
//            KafkaMbean mbean = this.mbeans.get(mBeanName);
//            mbean.setAttribute(metricName.name(), metric);
//            return mbean;
//        } catch (JMException e) {
//            throw new KafkaException("Error creating mbean attribute for metricName :" + metric.metricName(), e);
//        }
//    }

//    /**
//     * @param metricName
//     * @return standard JMX MBean name in the following format domainName:type=metricType,key1=val1,key2=val2
//     */
//    static string getMBeanName(string prefix, MetricName metricName) {
//        StringBuilder mBeanName = new StringBuilder();
//        mBeanName.append(prefix);
//        mBeanName.append(":type=");
//        mBeanName.append(metricName.group());
//        for (Map.Entry<string, string> entry : metricName.tags().entrySet()) {
//            if (entry.getKey().length() <= 0 || entry.getValue().length() <= 0)
//                continue;
//            mBeanName.append(",");
//            mBeanName.append(entry.getKey());
//            mBeanName.append("=");
//            mBeanName.append(Sanitizer.jmxSanitize(entry.getValue()));
//        }
//        return mBeanName.toString();
//    }

//    public void close() {
//        synchronized (LOCK) {
//            for (KafkaMbean mbean : this.mbeans.values())
//                unregister(mbean);
//        }
//    }

//    private void unregister(KafkaMbean mbean) {
//        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
//        try {
//            if (server.isRegistered(mbean.name()))
//                server.unregisterMBean(mbean.name());
//        } catch (JMException e) {
//            throw new KafkaException("Error unregistering mbean", e);
//        }
//    }

//    private void reregister(KafkaMbean mbean) {
//        unregister(mbean);
//        try {
//            ManagementFactory.getPlatformMBeanServer().registerMBean(mbean, mbean.name());
//        } catch (JMException e) {
//            throw new KafkaException("Error registering mbean " + mbean.name(), e);
//        }
//    }

//    private static class KafkaMbean implements DynamicMBean {
//        private ObjectName objectName;
//        private Dictionary<string, KafkaMetric> metrics;

//        public KafkaMbean(string mbeanName) throws MalformedObjectNameException {
//            this.metrics = new HashMap<>();
//            this.objectName = new ObjectName(mbeanName);
//        }

//        public ObjectName name() {
//            return objectName;
//        }

//        public void setAttribute(string name, KafkaMetric metric) {
//            this.metrics.put(name, metric);
//        }

//        @Override
//        public object getAttribute(string name) throws AttributeNotFoundException, MBeanException, ReflectionException {
//            if (this.metrics.containsKey(name))
//                return this.metrics.get(name).metricValue();
//            else
//                throw new AttributeNotFoundException("Could not find attribute " + name);
//        }

//        @Override
//        public AttributeList getAttributes(string[] names) {
//            AttributeList list = new AttributeList();
//            for (string name : names) {
//                try {
//                    list.add(new Attribute(name, getAttribute(name)));
//                } catch (Exception e) {
//                    log.warn("Error getting JMX attribute '{}'", name, e);
//                }
//            }
//            return list;
//        }

//        public KafkaMetric removeAttribute(string name) {
//            return this.metrics.remove(name);
//        }

//        @Override
//        public MBeanInfo getMBeanInfo() {
//            MBeanAttributeInfo[] attrs = new MBeanAttributeInfo[metrics.size()];
//            int i = 0;
//            for (Map.Entry<string, KafkaMetric> entry : this.metrics.entrySet()) {
//                string attribute = entry.getKey();
//                KafkaMetric metric = entry.getValue();
//                attrs[i] = new MBeanAttributeInfo(attribute,
//                                                  double.class.getName(),
//                                                  metric.metricName().description(),
//                                                  true,
//                                                  false,
//                                                  false);
//                i += 1;
//            }
//            return new MBeanInfo(this.GetType().getName(), "", attrs, null, null, null);
//        }

//        @Override
//        public object invoke(string name, Object[] params, string[] sig) throws MBeanException, ReflectionException {
//            throw new UnsupportedOperationException("Set not allowed.");
//        }

//        @Override
//        public void setAttribute(Attribute attribute) throws AttributeNotFoundException,
//                                                     InvalidAttributeValueException,
//                                                     MBeanException,
//                                                     ReflectionException {
//            throw new UnsupportedOperationException("Set not allowed.");
//        }

//        @Override
//        public AttributeList setAttributes(AttributeList list) {
//            throw new UnsupportedOperationException("Set not allowed.");
//        }

//    }

//}
