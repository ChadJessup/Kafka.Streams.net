using Kafka.Streams.Interfaces;

namespace Kafka.Streams.State.Metered
{
    internal class StateSerdes<T> : StateSerdes<object, object>
    {
        private object p;
        private ISerde<object> serde1;
        private ISerde<object> serde2;

        public StateSerdes(object p, ISerde<object> serde1, ISerde<object> serde2)
        {
            this.p = p;
            this.serde1 = serde1;
            this.serde2 = serde2;
        }
    }
}