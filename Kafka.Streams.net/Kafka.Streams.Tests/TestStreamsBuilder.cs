using Kafka.Streams.Kafka.Streams;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Tests
{
    public class TestStreamsBuilder : StreamsBuilder
    {
        protected override IServiceCollection AddClients(IServiceCollection serviceCollection)
        {
            return base.AddClients(serviceCollection);
        }
    }
}
