

namespace Kafka.Streams.KStream.Internals
{
    public class JoinedInternal<K, V, VO> : Joined<K, V, VO>
    {
        public JoinedInternal(Joined<K, V, VO> joined)
            : base(joined)
        {
        }

    }
}
