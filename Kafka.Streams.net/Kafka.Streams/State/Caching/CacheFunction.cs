namespace Kafka.Streams.State.Interfaces
{
    public interface ICacheFunction
    {
        Bytes Key(Bytes cacheKey);
        Bytes CacheKey(Bytes cacheKey);
    }
}