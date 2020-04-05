using Confluent.Kafka;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Streams.Processors.Internals
{
    /**
     * The result of the {@link Admin#deleteRecords(Dictionary)} call.
     *
     * The API of this class is evolving, see {@link Admin} for details.
     */
    public partial class DeleteRecordsResult
    {

        private readonly Dictionary<TopicPartition, Task<DeletedRecords>> tasks;

        public DeleteRecordsResult(Dictionary<TopicPartition, Task<DeletedRecords>> tasks)
        {
            this.tasks = tasks;
        }

        /**
         * Return a map from topic partition to futures which can be used to check the status of
         * individual deletions.
         */
        public Dictionary<TopicPartition, Task<DeletedRecords>> LowWatermarks()
        {
            return tasks;
        }

        /**
         * Return a future which succeeds only if all the records deletions succeed.
         */
        public Task All()
        {
            return Task.WhenAll(tasks.Values.ToArray());
        }
    }
}