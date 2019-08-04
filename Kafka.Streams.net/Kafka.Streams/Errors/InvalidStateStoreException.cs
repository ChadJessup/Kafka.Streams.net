namespace Kafka.Streams.Errors
{
/**
 * Indicates that there was a problem when trying to access a
 * {@link org.apache.kafka.streams.processor.IStateStore IStateStore}, i.e, the Store is no longer valid because it is
 * closed or doesn't exist any more due to a rebalance.
 * <p>
 * These exceptions may be transient, i.e., during a rebalance it won't be possible to query the stores as they are
 * being (re)-initialized. Once the rebalance has completed the stores will be available again. Hence, it is valid
 * to backoff and retry when handling this exception.
 */
public class InvalidStateStoreException : StreamsException
    {

    public InvalidStateStoreException( string message)
{
        base(message);
    }

    public InvalidStateStoreException( string message,  Throwable throwable)
{
        base(message, throwable);
    }

    public InvalidStateStoreException( Throwable throwable)
{
        base(throwable);
    }
}
