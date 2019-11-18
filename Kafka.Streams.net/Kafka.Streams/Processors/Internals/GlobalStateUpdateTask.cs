

//        public Dictionary<TopicPartition, long> initialize()
//        {
//            HashSet<string> storeNames = stateMgr.initialize();
//            Dictionary<string, string> storeNameToTopic = topology.storeToChangelogTopic();
//            foreach (string storeName in storeNames)
//            {
//                string sourceTopic = storeNameToTopic[storeName];
//                SourceNode source = topology.source(sourceTopic);
//                deserializers.Add(
//                    sourceTopic,
//                    new RecordDeserializer(
//                        source,
//                        deserializationExceptionHandler,
//                        logContext,
//                        ThreadMetrics.skipRecordSensor(processorContext.metrics)
//                    )
//                );
//            }
//            initTopology();
//            processorContext.initialize();
//            return stateMgr.checkpointed();
//        }



//        public void update(ConsumeResult<byte[], byte[]> record)
//        {
//            RecordDeserializer sourceNodeAndDeserializer = deserializers[record.Topic];
//            ConsumeResult<object, object> deserialized = sourceNodeAndDeserializer.Deserialize(processorContext, record);

//            if (deserialized != null)
//            {
//                ProcessorRecordContext recordContext =
//                    new ProcessorRecordContext(deserialized.timestamp(),
//                        deserialized.offset(),
//                        deserialized.partition(),
//                        deserialized.Topic,
//                        deserialized.headers());
//                processorContext.setRecordContext(recordContext);
//                processorContext.setCurrentNode(sourceNodeAndDeserializer.sourceNode());
//                sourceNodeAndDeserializer.sourceNode().process(deserialized.key(), deserialized.value());
//            }

//            offsets.Add(new TopicPartition(record.Topic, record.partition()), record.offset() + 1);
//        }

//        public void flushState()
//        {
//            stateMgr.flush();
//            stateMgr.checkpoint(offsets);
//        }

//        public void close()
//        {
//            stateMgr.close(true);
//        }

//        private void initTopology()
//        {
//            foreach (ProcessorNode node in this.topology.processors())
//            {
//                processorContext.setCurrentNode(node);
//                try
//                {

//                    node.init(this.processorContext);
//                }
//                finally
//                {

//                    processorContext.setCurrentNode(null);
//                }
//            }
//        }
//    }
//}