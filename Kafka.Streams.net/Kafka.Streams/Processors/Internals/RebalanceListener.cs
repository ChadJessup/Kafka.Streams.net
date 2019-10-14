using Confluent.Kafka;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Tasks;
using Kafka.Streams.Threads.KafkaStream;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class RebalanceListener : IConsumerRebalanceListener
    {
        private readonly ITime time;
        private readonly TaskManager taskManager;
        private readonly KafkaStreamThread streamThread;
        private readonly ILogger log;

        public RebalanceListener(
            ITime time,
            TaskManager taskManager,
            KafkaStreamThread streamThread,
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
                streamThread.State,
                assignment,
                taskManager.suspendedActiveTaskIds(),
                taskManager.suspendedStandbyTaskIds());

            if (streamThread.AssignmentErrorCode == (int)StreamsPartitionAssignor.Error.INCOMPLETE_SOURCE_TOPIC_METADATA)
            {
                log.LogError("Received error code {} - shutdown", streamThread.AssignmentErrorCode);
                streamThread.Shutdown();
                return;
            }
            long start = time.milliseconds();
            try
            {

                if (streamThread.State.SetState(KafkaStreamThreadStates.PARTITIONS_ASSIGNED) == null)
                {
                    log.LogDebug(
                        "Skipping task creation in rebalance because we are already in {} state.",
                        streamThread.State
                    );
                }
                else if (streamThread.AssignmentErrorCode != (int)StreamsPartitionAssignor.Error.NONE)
                {
                    log.LogDebug(
                        "Encountered assignment error during partition assignment: {}. Skipping task initialization",
                        streamThread.AssignmentErrorCode
                    );
                }
                else
                {

                    log.LogDebug("Creating tasks based on assignment.");
                    taskManager.createTasks(assignment);
                }
            }
            catch (Exception t)
            {
                log.LogError(
                    "Error caught during partition assignment, " +
                        "will abort the current process and re-throw at the end of rebalance", t);
                streamThread.SetRebalanceException(t);
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
                streamThread.State,
                assignment,
                taskManager.activeTaskIds(),
                taskManager.standbyTaskIds());

            if (streamThread.State.SetState(KafkaStreamThreadStates.PARTITIONS_REVOKED))
            {
                long start = time.milliseconds();
                try
                {

                    // suspend active tasks
                    if (streamThread.AssignmentErrorCode == (int)StreamsPartitionAssignor.Error.VERSION_PROBING)
                    {
                        streamThread.AssignmentErrorCode = (int)StreamsPartitionAssignor.Error.NONE;
                    }
                    else
                    {

                        taskManager.suspendTasksAndState();
                    }
                }
                catch (Exception t)
                {
                    log.LogError(
                        "Error caught during partition revocation, " +
                            "will abort the current process and re-throw at the end of rebalance: {}",
                        t
                    );
                    streamThread.SetRebalanceException(t);
                }
                finally
                {

                    streamThread.ClearStandbyRecords();

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
