using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Kafka.Streams.Clients.Consumers
{
    public class BaseConsumer<K, V> : IConsumer<K, V>
    {
        private readonly IConsumer<K, V> consumer;
        private readonly ILogger<BaseConsumer<K, V>> logger;

        private bool disposedValue = false; // To detect redundant calls

        public BaseConsumer(ILogger<BaseConsumer<K, V>> logger)
        {
            this.logger = logger;
        }

        public BaseConsumer(
            ILogger<BaseConsumer<K, V>> logger,
            IConsumer<K, V> consumer)
        {
            this.logger = logger;
            this.consumer = consumer;
        }

        public BaseConsumer(
            ILogger<BaseConsumer<K, V>> logger,
            ConsumerConfig? configs,
            ConsumerBuilder<K, V>? builder = null)
        {
            this.logger = logger;
            builder ??= new ConsumerBuilder<K, V>(configs);

            this.consumer = builder.Build();
        }

        public virtual string MemberId => this.consumer.MemberId;
        public virtual List<TopicPartition> Assignment
        { 
            get => this.consumer.Assignment; 
        }

        public virtual List<string> Subscription => this.consumer.Subscription;
        public virtual Handle Handle => this.consumer.Handle;
        public virtual string Name => this.consumer.Name;

        public virtual int AddBrokers(string brokers)
            => this.consumer.AddBrokers(brokers);

        public virtual void Assign(TopicPartition partition)
            => this.consumer.Assign(partition);

        public virtual void Assign(TopicPartitionOffset partition)
            => this.consumer.Assign(partition);

        public virtual void Assign(IEnumerable<TopicPartitionOffset> partitions)
            => this.consumer.Assign(partitions);

        public virtual void Assign(IEnumerable<TopicPartition> partitions)
            => this.consumer.Assign(partitions);

        public virtual void Close()
            => this.consumer.Close();
        public virtual List<TopicPartitionOffset> Commit()
            => this.consumer.Commit();

        public virtual void Commit(IEnumerable<TopicPartitionOffset> offsets)
            => this.consumer.Commit(offsets);

        public virtual void Commit(ConsumeResult<K, V> result)
            => this.consumer.Commit(result);

        public virtual List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
            => this.consumer.Committed(partitions, timeout);

        public virtual ConsumeResult<K, V> Consume(CancellationToken cancellationToken = default)
            => this.consumer.Consume(cancellationToken);

        public virtual ConsumeResult<K, V> Consume(TimeSpan timeout)
            => this.consumer.Consume(timeout);

        public virtual WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
            => this.consumer.GetWatermarkOffsets(topicPartition);

        public virtual List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
            => this.consumer.OffsetsForTimes(timestampsToSearch, timeout);

        public virtual void Pause(IEnumerable<TopicPartition> partitions)
            => this.consumer.Pause(partitions);

        public virtual Offset Position(TopicPartition partition)
            => this.consumer.Position(partition);

        public virtual WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
            => this.consumer.QueryWatermarkOffsets(topicPartition, timeout);

        public virtual void Resume(IEnumerable<TopicPartition> partitions)
            => this.consumer.Resume(partitions);

        public virtual void Seek(TopicPartitionOffset tpo)
            => this.consumer.Seek(tpo);

        public virtual void StoreOffset(ConsumeResult<K, V> result)
            => this.consumer.StoreOffset(result);
        public virtual void StoreOffset(TopicPartitionOffset offset)
            => this.consumer.StoreOffset(offset);

        public virtual void Subscribe(IEnumerable<string> topics)
            => this.consumer.Subscribe(topics);

        public virtual void Subscribe(string topic)
            => this.consumer.Subscribe(topic);

        public virtual void Unassign()
            => this.consumer.Unassign();

        public virtual void Unsubscribe()
            => this.consumer.Unsubscribe();

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    this.consumer.Dispose();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~BaseConsumer()
        // {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
    }
}
