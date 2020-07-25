using System;
using Confluent.Kafka;

namespace TestApp
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                Debug = "cgrp",
                GroupId = "test",
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.Range,
                BootstrapServers = "localhost:9092",
            };

            var consumer = new ConsumerBuilder<Null, string>(config)
                .Build();

            consumer.Subscribe("test");

            var running = true;
            while (running)
            {
                var msg = consumer.Consume(TimeSpan.FromMinutes(10.0));
            }
        }
    }
}
