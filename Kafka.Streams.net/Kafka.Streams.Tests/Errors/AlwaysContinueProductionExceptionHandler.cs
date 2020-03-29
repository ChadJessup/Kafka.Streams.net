/*






 *

 *





 */





/**
 * Production exception handler that always instructs streams to continue when an exception
 * happens while attempting to produce result records.
 */
public class AlwaysContinueProductionExceptionHandler : ProductionExceptionHandler {
    
    public ProductionExceptionHandlerResponse Handle(ProducerRecord<byte[], byte[]> record,
                                                     Exception exception) {
        return ProductionExceptionHandlerResponse.CONTINUE;
    }

    
    public void Configure(Dictionary<string, ?> configs) {
        // ignore
    }
}
