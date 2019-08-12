using Confluent.Kafka;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Errors;
using Microsoft.Extensions.Logging;
using System.Collections.ObjectModel;

namespace Kafka.Streams.Processor.Internals
{
    public class RebalanceListener : ConsumerRebalanceListener
    {

        private ITime time;
        private TaskManager taskManager;
        private StreamThread streamThread;
        private ILogger log;

        RebalanceListener(ITime time,
                          TaskManager taskManager,
                          StreamThread streamThread,
                          ILogger log)
        {
            this.time = time;
            this.taskManager = taskManager;
            this.streamThread = streamThread;
            this.log = log;
        }


        public void onPartitionsAssigned(List<TopicPartition> assignment)
        {
            log.LogDebug("at state {}: partitions {} assigned at the end of consumer rebalance.\n" +
                    "\tcurrent suspended active tasks: {}\n" +
                    "\tcurrent suspended standby tasks: {}\n",
                streamThread.state,
                assignment,
                taskManager.suspendedActiveTaskIds(),
                taskManager.suspendedStandbyTaskIds());

            if (streamThread.assignmentErrorCode() == StreamsPartitionAssignor.Error.INCOMPLETE_SOURCE_TOPIC_METADATA.code())
            {
                log.LogError("Received error code {} - shutdown", streamThread.assignmentErrorCode());
                streamThread.shutdown();
                return;
            }
            long start = time.milliseconds();
            try
            {

                if (streamThread.setState(State.PARTITIONS_ASSIGNED) == null)
                {
                    log.LogDebug(
                        "Skipping task creation in rebalance because we are already in {} state.",
                        streamThread.state()
                    );
                }
                else if (streamThread.assignmentErrorCode() != StreamsPartitionAssignor.Error.NONE.code())
                {
                    log.LogDebug(
                        "Encountered assignment error during partition assignment: {}. Skipping task initialization",
                        streamThread.assignmentErrorCode
                    );
                }
                else
                {

                    log.LogDebug("Creating tasks based on assignment.");
                    taskManager.createTasks(assignment);
                }
            }
            catch (Throwable t)
            {
                log.LogError(
                    "Error caught during partition assignment, " +
                        "will abort the current process and re-throw at the end of rebalance", t);
                streamThread.setRebalanceException(t);
            }
            finally
            {

                log.LogInformation("partition assignment took {} ms.\n" +
                        "\tcurrent active tasks: {}\n" +
                        "\tcurrent standby tasks: {}\n" +
                        "\tprevious active tasks: {}\n",
                    time.milliseconds() - start,
                    taskManager.activeTaskIds(),
                    taskManager.standbyTaskIds(),
                    taskManager.prevActiveTaskIds());
            }
        }


        public void onPartitionsRevoked(List<TopicPartition> assignment)
        {
            log.LogDebug("at state {}: partitions {} revoked at the beginning of consumer rebalance.\n" +
                    "\tcurrent assigned active tasks: {}\n" +
                    "\tcurrent assigned standby tasks: {}\n",
                streamThread.state,
                assignment,
                taskManager.activeTaskIds(),
                taskManager.standbyTaskIds());

            if (streamThread.setState(State.PARTITIONS_REVOKED) != null)
            {
                long start = time.milliseconds();
                try
                {

                    // suspend active tasks
                    if (streamThread.assignmentErrorCode() == StreamsPartitionAssignor.Error.VERSION_PROBING.code())
                    {
                        streamThread.assignmentErrorCode.set(StreamsPartitionAssignor.Error.NONE.code());
                    }
                    else
                    {

                        taskManager.suspendTasksAndState();
                    }
                }
                catch (Throwable t)
                {
                    log.LogError(
                        "Error caught during partition revocation, " +
                            "will abort the current process and re-throw at the end of rebalance: {}",
                        t
                    );
                    streamThread.setRebalanceException(t);
                }
                finally
                {

                    streamThread.clearStandbyRecords();

                    log.LogInformation("partition revocation took {} ms.\n" +
                            "\tsuspended active tasks: {}\n" +
                            "\tsuspended standby tasks: {}",
                        time.milliseconds() - start,
                        taskManager.suspendedActiveTaskIds(),
                        taskManager.suspendedStandbyTaskIds());
                }
            }
        }
    }
}
