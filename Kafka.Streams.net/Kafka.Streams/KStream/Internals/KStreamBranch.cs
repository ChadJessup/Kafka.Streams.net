using Kafka.Streams.Processors;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamBranch<K, V> : IProcessorSupplier<K, V>
    {
        private readonly Func<K, V, bool>[] predicates;
        private readonly string[] childNodes;

        public KStreamBranch(
            Func<K, V, bool>[] predicates,
            string[] childNodes)
        {
            this.predicates = predicates;
            this.childNodes = childNodes;
        }

        public IKeyValueProcessor<K, V> Get()
            => new KStreamBranchProcessor<K, V>();

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}
