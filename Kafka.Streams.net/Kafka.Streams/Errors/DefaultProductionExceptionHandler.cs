using Kafka.Streams.Errors.Interfaces;

namespace Kafka.Streams.Errors
{
/**
 * {@code ProductionExceptionHandler} that always instructs streams to fail when an exception
 * happens while attempting to produce result records.
 */
public class DefaultProductionExceptionHandler : IProductionExceptionHandler
    {

    public ProductionExceptionHandlerResponse handle( ProducerRecord<byte[], byte[]> record,
                                                      Exception exception)
{
        return ProductionExceptionHandlerResponse.FAIL;
    }


    public void configure( Map<string, object> configs)
{
        // ignore
    }
}
