using Confluent.Kafka;
using Kafka.Streams.Errors.Interfaces;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class GlobalStateUpdateTask : IGlobalStateMaintainer
    {
        private ProcessorTopology topology;
        private IInternalProcessorContext processorContext;
        private Dictionary<TopicPartition, long> offsets = new Dictionary<TopicPartition, long>();
        private Dictionary<string, RecordDeserializer> deserializers = new Dictionary<string, RecordDeserializer>();
        private GlobalStateManager stateMgr;
        private IDeserializationExceptionHandler deserializationExceptionHandler;

        public GlobalStateUpdateTask(
            ProcessorTopology topology,
            IInternalProcessorContext processorContext,
            GlobalStateManager stateMgr,
            IDeserializationExceptionHandler deserializationExceptionHandler)
        {
            this.topology = topology;
            this.stateMgr = stateMgr;
            this.processorContext = processorContext;
            this.deserializationExceptionHandler = deserializationExceptionHandler;
        }

        public Dictionary<TopicPartition, long?> initialize()
        {
            HashSet<string> storeNames = stateMgr.Initialize();
            Dictionary<string, string> storeNameToTopic = topology.StoreToChangelogTopic;

            foreach (string storeName in storeNames)
            {
                string sourceTopic = storeNameToTopic[storeName];
                ISourceNode source = topology.Source(sourceTopic);
                deserializers.Add(
                    sourceTopic,
                    new RecordDeserializer(
                        null,
                        source,
                        deserializationExceptionHandler)
                );
            }

            initTopology();
            processorContext.initialize();
            return stateMgr.checkpointed();
        }

        public void update(ConsumeResult<byte[], byte[]> record)
        {
            var sourceNodeAndDeserializer = deserializers[record.Topic];
            ConsumeResult<object, object> deserialized = sourceNodeAndDeserializer.Deserialize<object, object>(processorContext, record);

            if (deserialized != null)
            {
                var recordContext =
                    new ProcessorRecordContext(deserialized.Timestamp.UnixTimestampMs,
                        deserialized.Offset,
                        deserialized.Partition,
                        deserialized.Topic,
                        deserialized.Headers);
                processorContext.setRecordContext(recordContext);
                processorContext.SetCurrentNode(sourceNodeAndDeserializer.SourceNode);

                sourceNodeAndDeserializer.SourceNode.Process(deserialized.Key, deserialized.Value);
            }

            this.offsets.Add(new TopicPartition(record.Topic, record.Partition), record.Offset + 1);
        }

        public void flushState()
        {
            stateMgr.Flush();
            stateMgr.checkpoint(offsets);
        }

        public void close()
        {
            stateMgr.Close(true);
        }

        private void initTopology()
        {
            foreach (ProcessorNode node in this.topology.processors())
            {
                processorContext.SetCurrentNode(node);

                try
                {
                    node.Init(this.processorContext);
                }
                finally
                {
                    processorContext.SetCurrentNode(null);
                }
            }
        }
    }
}
