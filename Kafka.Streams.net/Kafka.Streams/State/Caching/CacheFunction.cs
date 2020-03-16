namespace Kafka.Streams.State.Interfaces
{
    public interface ICacheFunction
    {
        Bytes key(Bytes cacheKey);
        Bytes cacheKey(Bytes cacheKey);
    }
}