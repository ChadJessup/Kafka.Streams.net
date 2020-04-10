using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Common;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams
{
    /// <summary>
    /// Context object that is built by the <seealso cref="StreamsBuilder"/>.
    /// This is injected into many components, and contains various thingys,
    /// that will be needed All over the place.
    /// </summary>
    public class KafkaStreamsContext : IServiceProvider, IDisposable, IAsyncDisposable
    {
        private readonly CancellationTokenSource cts = new CancellationTokenSource();

        public KafkaStreamsContext(
            InternalTopologyBuilder internalTopologyBuilder,
            IServiceCollection serviceCollection,
            ILogger<KafkaStreamsContext> logger,
            IConfiguration configuration,
            IStoresFactory storesFactory,
            ILoggerFactory loggerFactory,
            Topology topology,
            IClock clock)
        {
            this.Clock = clock;
            this.Logger = logger;
            this.Topology = topology;
            this.Configuration = configuration;
            this.StoresFactory = storesFactory;
            this.LoggerFactory = loggerFactory;
            this.ServiceCollection = serviceCollection;
            this.InternalTopologyBuilder = internalTopologyBuilder;

            this.Services = this.ServiceCollection.BuildServiceProvider();
        }

        public InternalStreamsBuilder InternalStreamsBuilder { get; internal set; }
        public InternalTopologyBuilder InternalTopologyBuilder { get; }
        public IServiceCollection ServiceCollection { get; }
        public ILogger<KafkaStreamsContext> Logger { get; }
        public IConfiguration Configuration { get; }
        public ILoggerFactory LoggerFactory { get; }
        public ServiceProvider Services { get; }
        public Topology Topology { get; }
        public IStoresFactory StoresFactory { get; }
        public IClock Clock { get; }

        public ILogger<TCategory> CreateLogger<TCategory>()
            => this.LoggerFactory.CreateLogger<TCategory>();

        public object? GetService<T>() => this.Services.GetService<T>();
        public object GetService(Type serviceType) => this.Services.GetService(serviceType);
        public T GetRequiredService<T>() => this.Services.GetRequiredService<T>();
        public IServiceScope CreateScope() => this.Services.CreateScope();

        private bool disposedValue = false;
        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposedValue)
            {
                if (disposing)
                {
                    this.LoggerFactory?.Dispose();
                    this.Services?.Dispose();
                }

                this.disposedValue = true;
            }
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        public async ValueTask DisposeAsync()
        {
            this.Services?.DisposeAsync();

            await Task.CompletedTask;
        }
    }
}
