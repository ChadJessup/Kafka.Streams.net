using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Streams.Clients.Producers
{
    public class BaseProducer<K, V> : IProducer<K, V>
    {
        private readonly IProducer<K, V> producer;
        private readonly ILogger<BaseProducer<K, V>> logger;

        private bool disposedValue = false; // To detect redundant calls

        public Handle Handle => this.producer.Handle;
        public string Name => this.producer?.Name ?? "";

        public BaseProducer(ILogger<BaseProducer<K, V>> logger)
        {
            this.logger = logger;
        }

        public BaseProducer(
            ILogger<BaseProducer<K, V>> logger,
            IProducer<K, V> consumer)
        {
            this.logger = logger;
            this.producer = consumer;
        }

        public BaseProducer(
            ILogger<BaseProducer<K, V>> logger,
            ProducerConfig? configs,
            ProducerBuilder<K, V>? builder = null)
        {
            this.logger = logger;
            builder ??= new ProducerBuilder<K, V>(configs);

            this.producer = builder.Build();
        }

        public Task<DeliveryResult<K, V>> ProduceAsync(string topic, Message<K, V> message)
            => this.producer.ProduceAsync(topic, message);

        public Task<DeliveryResult<K, V>> ProduceAsync(TopicPartition topicPartition, Message<K, V> message)
            => this.producer.ProduceAsync(topicPartition, message);

        public void Produce(string topic, Message<K, V> message, Action<DeliveryReport<K, V>>? deliveryHandler = null)
            => this.producer.Produce(topic, message, deliveryHandler);

        public void Produce(TopicPartition topicPartition, Message<K, V> message, Action<DeliveryReport<K, V>>? deliveryHandler = null)
            => this.producer.Produce(topicPartition, message, deliveryHandler);

        public int Poll(TimeSpan timeout)
            => this.producer.Poll(timeout);

        public int Flush(TimeSpan timeout)
            => this.producer.Flush(timeout);

        public void Flush(CancellationToken cancellationToken = default)
            => this.producer.Flush(cancellationToken);

        public int AddBrokers(string brokers)
            => this.producer.AddBrokers(brokers);

        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposedValue)
            {
                if (disposing)
                {
                    this.producer?.Dispose();
                }

                this.disposedValue = true;
            }
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        public Task<DeliveryResult<K, V>> ProduceAsync(string topic, Message<K, V> message, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<DeliveryResult<K, V>> ProduceAsync(TopicPartition topicPartition, Message<K, V> message, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public void InitTransactions(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public void BeginTransaction()
        {
            throw new NotImplementedException();
        }

        public void CommitTransaction(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public void AbortTransaction(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }
    }
}
