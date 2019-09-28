namespace Kafka.Streams.Processor.Internals
{
    public partial class DeleteRecordsResult
    {
        /**
         * Represents information about deleted records
         *
         * The API for this class is still evolving and we may break compatibility in minor releases, if necessary.
         */
        public class DeletedRecords
        {

            /**
             * Return the "low watermark" for the topic partition on which the deletion was executed
             */
            public long lowWatermark { get; }

            /**
             * Create an instance of this class with the provided parameters.
             *
             * @param lowWatermark  "low watermark" for the topic partition on which the deletion was executed
             */
            public DeletedRecords(long lowWatermark)
            {
                this.lowWatermark = lowWatermark;
            }

        }
    }
}