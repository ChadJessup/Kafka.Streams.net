using Kafka.Streams.Kafka.Streams;
using Microsoft.Extensions.DependencyInjection;

namespace Kafka.Streams.Tests.Helpers
{
    public class TestStreamsBuilder : StreamsBuilder
    {
        protected override IServiceCollection AddClients(IServiceCollection serviceCollection)
        {
            return base.AddClients(serviceCollection);
        }
    }
}
