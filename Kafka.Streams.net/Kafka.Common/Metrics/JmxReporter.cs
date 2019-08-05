/////*
//// * Licensed to the Apache Software Foundation (ASF) under one or more
//// * contributor license agreements. See the NOTICE file distributed with
//// * this work for.Additional information regarding copyright ownership.
//// * The ASF licenses this file to You under the Apache License, Version 2.0
//// * (the "License"); you may not use this file except in compliance with
//// * the License. You may obtain a copy of the License at
//// *
//// *    http://www.apache.org/licenses/LICENSE-2.0
//// *
//// * Unless required by applicable law or agreed to in writing, software
//// * distributed under the License is distributed on an "AS IS" BASIS,
//// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// * See the License for the specific language governing permissions and
//// * limitations under the License.
//// */
////namespace Kafka.Common.Metrics

////
////
////
////

////
////
////
////
////
////
////
////
////
////
////
////
////

////using Kafka.Common.KafkaException;
////
////using Kafka.Common.Utils.Sanitizer;
////
////

/////**
//// * Register metrics in JMX as dynamic mbeans based on the metric names
//// */
////public JmxReporter : MetricsReporter
////{


////    private static ILogger log = new LoggerFactory().CreateLogger<JmxReporter);
////    private static object LOCK = new object();
////    private string prefix;
////    private Dictionary<string, KafkaMbean> mbeans = new HashMap<string, KafkaMbean>();

////    public JmxReporter()
//{
////        this("");
////    }

////    /**
////     * Create a JMX reporter that prefixes all metrics with the given string.
////     */
////    public JmxReporter(string prefix)
//{
////        this.prefix = prefix;
////    }

////
////    public void configure(Dictionary<string, object> configs) {}

////
////    public void init(List<KafkaMetric> metrics)
//{
////        synchronized (LOCK)
//{
////            foreach (KafkaMetric metric in metrics)
////               .AddAttribute(metric);
////            foreach (KafkaMbean mbean in mbeans.values())
////                reregister(mbean);
////        }
////    }

////    public bool containsMbean(string mbeanName)
//{
////        return mbeans.ContainsKey(mbeanName);
////    }
////
////    public void metricChange(KafkaMetric metric)
//{
////        synchronized (LOCK)
//{
////            KafkaMbean mbean =.AddAttribute(metric);
////            reregister(mbean);
////        }
////    }

////
////    public void metricRemoval(KafkaMetric metric)
//{
////        synchronized (LOCK)
//{
////            MetricName metricName = metric.metricName;
////            string mBeanName = getMBeanName(prefix, metricName);
////            KafkaMbean mbean = removeAttribute(metric, mBeanName);
////            if (mbean != null)
//{
////                if (mbean.metrics.isEmpty())
//{
////                    unregister(mbean);
////                    mbeans.Remove(mBeanName);
////                } else
////                    reregister(mbean);
////            }
////        }
////    }

////    private KafkaMbean removeAttribute(KafkaMetric metric, string mBeanName)
//{
////        MetricName metricName = metric.metricName;
////        KafkaMbean mbean = this.mbeans[mBeanName];
////        if (mbean != null)
////            mbean.removeAttribute(metricName.name());
////        return mbean;
////    }

////    private KafkaMbean.AddAttribute(KafkaMetric metric)
//{
////        try
//{

////            MetricName metricName = metric.metricName;
////            string mBeanName = getMBeanName(prefix, metricName);
////            if (!this.mbeans.ContainsKey(mBeanName))
////                mbeans.Add(mBeanName, new KafkaMbean(mBeanName));
////            KafkaMbean mbean = this.mbeans[mBeanName];
////            mbean.setAttribute(metricName.name(), metric);
////            return mbean;
////        } catch (JMException e)
//{
////            throw new KafkaException("Error creating mbean attribute for metricName :" + metric.metricName, e);
////        }
////    }

////    /**
////     * @param metricName
////     * @return standard JMX MBean name in the following format domainName:type=metricType,key1=val1,key2=val2
////     */
////    static string getMBeanName(string prefix, MetricName metricName)
//{
////        StringBuilder mBeanName = new StringBuilder();
////        mBeanName.Append(prefix);
////        mBeanName.Append(":type=");
////        mBeanName.Append(metricName.group());
////        foreach (Map.Entry<string, string> entry in metricName.tags().entrySet())
//{
////            if (entry.Key.Length <= 0 || entry.Value.Length <= 0)
////                continue;
////            mBeanName.Append(",");
////            mBeanName.Append(entry.Key);
////            mBeanName.Append("=");
////            mBeanName.Append(Sanitizer.jmxSanitize(entry.Value));
////        }
////        return mBeanName.ToString();
////    }

////    public void close()
//{
////        synchronized (LOCK)
//{
////            foreach (KafkaMbean mbean in this.mbeans.values())
////                unregister(mbean);
////        }
////    }

////    private void unregister(KafkaMbean mbean)
//{
////        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
////        try
//{

////            if (server.isRegistered(mbean.name()))
////                server.unregisterMBean(mbean.name());
////        } catch (JMException e)
//{
////            throw new KafkaException("Error unregistering mbean", e);
////        }
////    }

////    private void reregister(KafkaMbean mbean)
//{
////        unregister(mbean);
////        try
//{

////            ManagementFactory.getPlatformMBeanServer().registerMBean(mbean, mbean.name());
////        } catch (JMException e)
//{
////            throw new KafkaException("Error registering mbean " + mbean.name(), e);
////        }
////    }

////    private static KafkaMbean : DynamicMBean
//{

////        private ObjectName objectName;
////        private Dictionary<string, KafkaMetric> metrics;

////        public KafkaMbean(string mbeanName){
////            this.metrics = new HashMap<>();
////            this.objectName = new ObjectName(mbeanName);
////        }

////        public ObjectName name()
//{
////            return objectName;
////        }

////        public void setAttribute(string name, KafkaMetric metric)
//{
////            this.metrics.Add(name, metric);
////        }

////
////        public object getAttribute(string name){
////            if (this.metrics.ContainsKey(name))
////                return this.metrics[name).metricValue();
////            else
////                throw new AttributeNotFoundException("Could not find attribute " + name);
////        }

////
////        public AttributeList getAttributes(string[] names)
//{
////            AttributeList list = new AttributeList();
////            foreach (string name in names)
//{
////                try
//{

////                    list.Add(new Attribute(name, getAttribute(name)));
////                } catch (Exception e)
//{
////                    log.LogWarning("Error getting JMX attribute '{}'", name, e);
////                }
////            }
////            return list;
////        }

////        public KafkaMetric removeAttribute(string name)
//{
////            return this.metrics.Remove(name);
////        }

////
////        public MBeanInfo getMBeanInfo()
//{
////            MBeanAttributeInfo[] attrs = new MBeanAttributeInfo[metrics.size());
////            int i = 0;
////            foreach (Map.Entry<string, KafkaMetric> entry in this.metrics.entrySet())
//{
////                string attribute = entry.Key;
////                KafkaMetric metric = entry.Value;
////                attrs[i] = new MBeanAttributeInfo(attribute,
////                                                  double.getName(),
////                                                  metric.metricName.description(),
////                                                  true,
////                                                  false,
////                                                  false);
////                i += 1;
////            }
////            return new MBeanInfo(this.GetType().getName(), "", attrs, null, null, null);
////        }

////
////        public object invoke(string name, object[] params, string[] sig]{
////            throw new InvalidOperationException("Set not allowed.");
////        }

////
////        public void setAttribute(Attribute attribute) throws AttributeNotFoundException,
////                                                     InvalidAttributeValueException,
////                                                     MBeanException,
////                                                     ReflectionException
//{

////            throw new InvalidOperationException("Set not allowed.");
////        }

////
////        public AttributeList setAttributes(AttributeList list)
//{
////            throw new InvalidOperationException("Set not allowed.");
////        }

////    }

////}
