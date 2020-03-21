using Confluent.Kafka;
using Kafka.Streams.Processors.Internals;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Streams.Tests.Mocks
{
    /**
     * A mock of the producer interface you can use for testing code that uses Kafka.
     * <p>
     * By default this mock will synchronously complete each send call successfully. However it can be configured to allow
     * the user to control the completion of the call and supply an optional error for the producer to throw.
     */
    public class MockProducer<K, V> : IProducer<K, V>
    {
        private Partitioner? partitioner;
        private List<Message<K, V>> sent;
        private List<Message<K, V>> uncommittedSends;
        //private Deque<Completion> completions;
        private Dictionary<TopicPartition, long> offsets;
        private List<Dictionary<string, Dictionary<TopicPartition, OffsetAndMetadata>>> consumerGroupOffsets;
        private Dictionary<string, Dictionary<TopicPartition, OffsetAndMetadata>> uncommittedConsumerGroupOffsets;
        private ISerializer<K>? keySerializer;
        private ISerializer<V>? valueSerializer;
        private bool autoComplete;
        private bool closed;
        private bool transactionInitialized;
        private bool transactionInFlight;
        private bool transactionCommitted;
        private bool transactionAborted;
        private bool producerFenced;
        private bool producerFencedOnClose;
        private bool sentOffsets;
        private long commitCount = 0L;

        /**
         * Create a mock producer
         *
         * @param cluster The cluster holding metadata for this producer
         * @param autoComplete If true automatically complete all requests successfully and execute the callback. Otherwise
         *        the user must call {@link #completeNext()} or {@link #errorNext(RuntimeException)} after
         *        {@link #send(Message) send()} to complete the call and unblock the {@link
         *        java.util.concurrent.Future Future&lt;RecordMetadata&gt;} that is returned.
         * @param partitioner The partition strategy
         * @param keySerializer The serializer for key that implements {@link Serializer}.
         * @param valueSerializer The serializer for value that implements {@link Serializer}.
         */
        public MockProducer(
            bool autoComplete,
            Partitioner? partitioner,
            ISerializer<K>? keySerializer,
            ISerializer<V>? valueSerializer)
        {
            this.autoComplete = autoComplete;
            this.partitioner = partitioner;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.offsets = new Dictionary<TopicPartition, long>();
            this.sent = new List<Message<K, V>>();
            this.uncommittedSends = new List<Message<K, V>>();
            this.consumerGroupOffsets = new List<Dictionary<string, Dictionary<TopicPartition, OffsetAndMetadata>>>();
            this.uncommittedConsumerGroupOffsets = new Dictionary<string, Dictionary<TopicPartition, OffsetAndMetadata>>();
        }

        /**
         * Create a new mock producer with invented metadata the given autoComplete setting and key\value serializers.
         *
         * Equivalent to {@link #MockProducer(Cluster, bool, Partitioner, Serializer, Serializer)} new MockProducer(Cluster.empty(), autoComplete, new DefaultPartitioner(), keySerializer, valueSerializer)}
         */
        public MockProducer(
            bool autoComplete,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer)
            : this(autoComplete, null, keySerializer, valueSerializer)
        {
        }

        /**
         * Create a new mock producer with invented metadata.
         *
         * Equivalent to {@link #MockProducer(Cluster, bool, Partitioner, Serializer, Serializer)} new MockProducer(Cluster.empty(), false, null, null, null)}
         */
        public MockProducer()
            : this(false, null, null, null)
        {
        }

        public Handle Handle { get; }
        public string Name { get; }

        public void InitTransactions()
        {
            // verifyProducerState();
            if (this.transactionInitialized)
            {
                throw new Exception("MockProducer has already been initialized for transactions.");
            }

            this.transactionInitialized = true;
        }

        public Task<DeliveryResult<K, V>> ProduceAsync(string topic, Message<K, V> message)
        {
            throw new NotImplementedException();
        }

        public Task<DeliveryResult<K, V>> ProduceAsync(TopicPartition topicPartition, Message<K, V> message)
        {
            throw new NotImplementedException();
        }

        public void Produce(string topic, Message<K, V> message, Action<DeliveryReport<K, V>> deliveryHandler = null)
        {
            throw new NotImplementedException();
        }

        public void Produce(TopicPartition topicPartition, Message<K, V> message, Action<DeliveryReport<K, V>> deliveryHandler = null)
        {
            throw new NotImplementedException();
        }

        public int Poll(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public int Flush(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public void Flush(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public int AddBrokers(string brokers)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        // {
        // 
        // ();
        //     verifyTransactionsInitialized();
        //     this.transactionInFlight = true;
        //     this.transactionCommitted = false;
        //     this.transactionAborted = false;
        //     this.sentOffsets = false;
        // }

        // @Override
        // public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
        //                                      string consumerGroupId) throws ProducerFencedException
        // {
        //     verifyProducerState();
        //     verifyTransactionsInitialized();
        //     verifyNoTransactionInFlight();
        //     Objects.requireNonNull(consumerGroupId);
        //     if (offsets.size() == 0) {
        //         return;
        //     }
        //     Map<TopicPartition, OffsetAndMetadata> uncommittedOffsets = this.uncommittedConsumerGroupOffsets.get(consumerGroupId);
        //     if (uncommittedOffsets == null)
        //     {
        //         uncommittedOffsets = new HashMap<>();
        //         this.uncommittedConsumerGroupOffsets.put(consumerGroupId, uncommittedOffsets);
        //     }
        //     uncommittedOffsets.putAll(offsets);
        //     this.sentOffsets = true;
        //     }
        // 
        //     @Override
        // public void commitTransaction() throws ProducerFencedException
        // {
        //     verifyProducerState();
        //     verifyTransactionsInitialized();
        //     verifyNoTransactionInFlight();
        // 
        //     flush();
        // 
        //     this.sent.addAll(this.uncommittedSends);
        //     if (!this.uncommittedConsumerGroupOffsets.isEmpty())
        //         this.consumerGroupOffsets.add(this.uncommittedConsumerGroupOffsets);
        // 
        //     this.uncommittedSends.clear();
        //     this.uncommittedConsumerGroupOffsets = new HashMap<>();
        //     this.transactionCommitted = true;
        //     this.transactionAborted = false;
        //     this.transactionInFlight = false;
        // 
        //     ++this.commitCount;
        //     }
        // 
        //     @Override
        // public void abortTransaction() throws ProducerFencedException
        // {
        //     verifyProducerState();
        //     verifyTransactionsInitialized();
        //     verifyNoTransactionInFlight();
        //     flush();
        //     this.uncommittedSends.clear();
        //     this.uncommittedConsumerGroupOffsets.clear();
        //     this.transactionCommitted = false;
        //     this.transactionAborted = true;
        //     this.transactionInFlight = false;
        // }
        // 
        // private synchronized void verifyProducerState()
        // {
        //     if (this.closed)
        //     {
        //         throw new IllegalStateException("MockProducer is already closed.");
        //     }
        //     if (this.producerFenced)
        //     {
        //         throw new ProducerFencedException("MockProducer is fenced.");
        //     }
        // }
        // 
        // private void verifyTransactionsInitialized()
        // {
        //     if (!this.transactionInitialized)
        //     {
        //         throw new IllegalStateException("MockProducer hasn't been initialized for transactions.");
        //     }
        // }
        // 
        // private void verifyNoTransactionInFlight()
        // {
        //     if (!this.transactionInFlight)
        //     {
        //         throw new IllegalStateException("There is no open transaction.");
        //     }
        // }
        // 
        // /**
        //  * Adds the record to the list of sent records. The {@link RecordMetadata} returned will be immediately satisfied.
        //  * 
        //  * @see #history()
        //  */
        // @Override
        // public synchronized Future<RecordMetadata> send(Message<K, V> record) {
        //     return send(record, null);
        // }
        // 
        // /**
        //  * Adds the record to the list of sent records.
        //  *
        //  * @see #history()
        //  */
        // @Override
        // public synchronized Future<RecordMetadata> send(Message<K, V> record, Callback callback) {
        //     if (this.closed)
        //     {
        //         throw new IllegalStateException("MockProducer is already closed.");
        //     }
        //     if (this.producerFenced)
        //     {
        //         throw new KafkaException("MockProducer is fenced.", new ProducerFencedException("Fenced"));
        //     }
        //     int partition = 0;
        //     if (!this.cluster.partitionsForTopic(record.topic()).isEmpty())
        //         partition = partition(record, this.cluster);
        //     TopicPartition topicPartition = new TopicPartition(record.topic(), partition);
        //     ProduceRequestResult result = new ProduceRequestResult(topicPartition);
        //     FutureRecordMetadata future = new FutureRecordMetadata(result, 0, RecordBatch.NO_TIMESTAMP,
        //             0L, 0, 0, Time.SYSTEM);
        //     long offset = nextOffset(topicPartition);
        //     Completion completion = new Completion(offset, new RecordMetadata(topicPartition, 0, offset,
        //             RecordBatch.NO_TIMESTAMP, long.valueOf(0L), 0, 0), result, callback);
        // 
        //     if (!this.transactionInFlight)
        //         this.sent.add(record);
        //     else
        //         this.uncommittedSends.add(record);
        // 
        //     if (autoComplete)
        //         completion.complete(null);
        //     else
        //         this.completions.addLast(completion);
        // 
        //     return future;
        // }
        // 
        // /**
        //  * Get the next offset for this topic/partition
        //  */
        // private long nextOffset(TopicPartition tp)
        // {
        //     long offset = this.offsets.get(tp);
        //     if (offset == null)
        //     {
        //         this.offsets.put(tp, 1L);
        //         return 0L;
        //     }
        //     else
        //     {
        //         long next = offset + 1;
        //         this.offsets.put(tp, next);
        //         return offset;
        //     }
        // }
        // 
        // public synchronized void flush()
        // {
        //     verifyProducerState();
        //     while (!this.completions.isEmpty())
        //         completeNext();
        // }
        // 
        // public List<PartitionInfo> partitionsFor(string topic)
        // {
        //     return this.cluster.partitionsForTopic(topic);
        // }
        // 
        // public Map<MetricName, Metric> metrics()
        // {
        //     return mockMetrics;
        // }
        // 
        // /**
        //  * Set a mock metric for testing purpose
        //  */
        // public void setMockMetrics(MetricName name, Metric metric)
        // {
        //     mockMetrics.put(name, metric);
        // }
        // 
        // @Override
        // public void close()
        // {
        //     close(Duration.ofMillis(0));
        // }
        // 
        // @Override
        // public void close(Duration timeout)
        // {
        //     if (producerFencedOnClose)
        //     {
        //         throw new ProducerFencedException("MockProducer is fenced.");
        //     }
        //     this.closed = true;
        // }
        // 
        // public bool closed()
        // {
        //     return this.closed;
        // }
        // 
        // public synchronized void fenceProducer()
        // {
        //     verifyProducerState();
        //     verifyTransactionsInitialized();
        //     this.producerFenced = true;
        // }
        // 
        // public void fenceProducerOnClose()
        // {
        //     verifyProducerState();
        //     verifyTransactionsInitialized();
        //     this.producerFencedOnClose = true;
        // }
        // 
        // public bool transactionInitialized()
        // {
        //     return this.transactionInitialized;
        // }
        // 
        // public bool transactionInFlight()
        // {
        //     return this.transactionInFlight;
        // }
        // 
        // public bool transactionCommitted()
        // {
        //     return this.transactionCommitted;
        // }
        // 
        // public bool transactionAborted()
        // {
        //     return this.transactionAborted;
        // }
        // 
        // public bool flushed()
        // {
        //     return this.completions.isEmpty();
        // }
        // 
        // public bool sentOffsets()
        // {
        //     return this.sentOffsets;
        // }
        // 
        // public long commitCount()
        // {
        //     return this.commitCount;
        // }
        // 
        // /**
        //  * Get the list of sent records since the last call to {@link #clear()}
        //  */
        // public synchronized List<Message<K, V>> history() {
        //     return new ArrayList<>(this.sent);
        // }
        // 
        // /**
        //  * Get the list of committed consumer group offsets since the last call to {@link #clear()}
        //  */
        // public synchronized List<Map<string, Map<TopicPartition, OffsetAndMetadata>>> consumerGroupOffsetsHistory() {
        //     return new ArrayList<>(this.consumerGroupOffsets);
        // }
        // /**
        //  *
        //  * Clear the stored history of sent records, consumer group offsets, and transactional state
        //  */
        // public synchronized void clear()
        // {
        //     this.sent.clear();
        //     this.uncommittedSends.clear();
        //     this.completions.clear();
        //     this.consumerGroupOffsets.clear();
        //     this.uncommittedConsumerGroupOffsets.clear();
        //     this.transactionInitialized = false;
        //     this.transactionInFlight = false;
        //     this.transactionCommitted = false;
        //     this.transactionAborted = false;
        //     this.producerFenced = false;
        // }

        /**
         * Complete the earliest uncompleted call successfully.
         *
         * @return true if there was an uncompleted call to complete
         */
        // public synchronized bool completeNext()
        // {
        //     return errorNext(null);
        // }

        /**
         * Complete the earliest uncompleted call with the given error.
         *
         * @return true if there was an uncompleted call to complete
         */
        // public synchronized bool errorNext(RuntimeException e)
        // {
        //     Completion completion = this.completions.pollFirst();
        //     if (completion != null)
        //     {
        //         completion.complete(e);
        //         return true;
        //     }
        //     else
        //     {
        //         return false;
        //     }
        // }

        /**
         * computes partition for given record.
         */
        //private int partition(Message<K, V> record, Cluster cluster)
        //{
        //    Integer partition = record.partition();
        //    string topic = record.topic();
        //    if (partition != null)
        //    {
        //        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        //        int numPartitions = partitions.size();
        //        // they have given us a partition, use it
        //        if (partition < 0 || partition >= numPartitions)
        //            throw new IllegalArgumentException("Invalid partition given with record: " + partition
        //                                               + " is not in the range [0..."
        //                                               + numPartitions
        //                                               + "].");
        //        return partition;
        //    }
        //    byte[] keyBytes = keySerializer.serialize(topic, record.headers(), record.key());
        //    byte[] valueBytes = valueSerializer.serialize(topic, record.headers(), record.value());
        //    return this.partitioner.partition(topic, record.key(), keyBytes, record.value(), valueBytes, cluster);
        //}
        //
        //private static class Completion
        //{
        //    private long offset;
        //    private RecordMetadata metadata;
        //    private ProduceRequestResult result;
        //    private Callback callback;
        //
        //    public Completion(long offset,
        //                      RecordMetadata metadata,
        //                      ProduceRequestResult result,
        //                      Callback callback)
        //    {
        //        this.metadata = metadata;
        //        this.offset = offset;
        //        this.result = result;
        //        this.callback = callback;
        //    }
        //
        //    public void complete(RuntimeException e)
        //    {
        //        result.set(e == null ? offset : -1L, RecordBatch.NO_TIMESTAMP, e);
        //        if (callback != null)
        //        {
        //            if (e == null)
        //                callback.onCompletion(metadata, null);
        //            else
        //                callback.onCompletion(null, e);
        //        }
        //        result.done();
        //    }
        //}
    }
}
