/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
namespace Kafka.Streams.Processor.Internals;



using Kafka.Common.KafkaException;
using Kafka.Common.TopicPartition;
using Kafka.Common.errors.AuthorizationException;
using Kafka.Common.errors.WakeupException;
using Kafka.Common.Utils.LogContext;
















public abstract class AbstractTask : Task
{


    TaskId id;
    string applicationId;
    ProcessorTopology topology;
    ProcessorStateManager stateMgr;
    HashSet<TopicPartition> partitions;
    IConsumer<byte[], byte[]> consumer;
    string logPrefix;
    bool eosEnabled;
    ILogger log;
    LogContext logContext;
    StateDirectory stateDirectory;

    bool taskInitialized;
    bool taskClosed;
    bool commitNeeded;

    IInternalProcessorContext processorContext;

    /**
     * @throws ProcessorStateException if the state manager cannot be created
     */
    AbstractTask(TaskId id,
                 Collection<TopicPartition> partitions,
                 ProcessorTopology topology,
                 IConsumer<byte[], byte[]> consumer,
                 ChangelogReader changelogReader,
                 bool isStandby,
                 StateDirectory stateDirectory,
                 StreamsConfig config)
{
        this.id = id;
        this.applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
        this.partitions = new HashSet<>(partitions);
        this.topology = topology;
        this.consumer = consumer;
        this.eosEnabled = StreamsConfig.EXACTLY_ONCE.Equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));
        this.stateDirectory = stateDirectory;

        this.logPrefix = string.Format("%s [%s] ", isStandby ? "standby-task" : "task", id);
        this.logContext = new LogContext(logPrefix);
        this.log = logContext.logger(GetType());

        // create the processor state manager
        try
{

            stateMgr = new ProcessorStateManager(
                id,
                partitions,
                isStandby,
                stateDirectory,
                topology.storeToChangelogTopic(),
                changelogReader,
                eosEnabled,
                logContext);
        } catch (IOException e)
{
            throw new ProcessorStateException(string.Format("%sError while creating the state manager", logPrefix), e);
        }
    }


    public TaskId id()
{
        return id;
    }


    public string applicationId()
{
        return applicationId;
    }


    public HashSet<TopicPartition> partitions()
{
        return partitions;
    }


    public ProcessorTopology topology()
{
        return topology;
    }


    public IProcessorContext context()
{
        return processorContext;
    }


    public IStateStore getStore(string name)
{
        return stateMgr.getStore(name);
    }

    /**
     * Produces a string representation containing useful information about a Task.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the StreamTask instance.
     */

    public string ToString()
{
        return ToString("");
    }

    public bool isEosEnabled()
{
        return eosEnabled;
    }

    /**
     * Produces a string representation containing useful information about a Task starting with the given indent.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the Task instance.
     */
    public string ToString(string indent)
{
        StringBuilder sb = new StringBuilder();
        sb.Append(indent);
        sb.Append("TaskId: ");
        sb.Append(id);
        sb.Append("\n");

        // print topology
        if (topology != null)
{
            sb.Append(indent).Append(topology.ToString(indent + "\t"));
        }

        // print assigned partitions
        if (partitions != null && !partitions.isEmpty())
{
            sb.Append(indent).Append("Partitions [");
            foreach (TopicPartition topicPartition in partitions)
{
                sb.Append(topicPartition.ToString()).Append(", ");
            }
            sb.setLength(sb.Length - 2);
            sb.Append("]\n");
        }
        return sb.ToString();
    }

    protected Dictionary<TopicPartition, long> activeTaskCheckpointableOffsets()
{
        return Collections.emptyMap();
    }

    protected void updateOffsetLimits()
{
        foreach (TopicPartition partition in partitions)
{
            try
{

                OffsetAndMetadata metadata = consumer.committed(partition); // TODO: batch API?
                long offset = metadata != null ? metadata.offset() : 0L;
                stateMgr.putOffsetLimit(partition, offset);

                if (log.isTraceEnabled())
{
                    log.LogTrace("Updating store offset limits {} for changelog {}", offset, partition);
                }
            } catch (AuthorizationException e)
{
                throw new ProcessorStateException(string.Format("task [%s] AuthorizationException when initializing offsets for %s", id, partition), e);
            } catch (WakeupException e)
{
                throw e;
            } catch (KafkaException e)
{
                throw new ProcessorStateException(string.Format("task [%s] Failed to initialize offsets for %s", id, partition), e);
            }
        }
    }

    /**
     * Flush all state stores owned by this task
     */
    void flushState()
{
        stateMgr.flush();
    }

    /**
     * Package-private for testing only
     *
     * @throws StreamsException If the store's change log does not contain the partition
     */
    void registerStateStores()
{
        if (topology.stateStores().isEmpty())
{
            return;
        }

        try
{

            if (!stateDirectory.lock(id))
{
                throw new LockException(string.Format("%sFailed to lock the state directory for task %s", logPrefix, id));
            }
        } catch (IOException e)
{
            throw new StreamsException(
                string.Format("%sFatal error while trying to lock the state directory for task %s",
                logPrefix, id));
        }
        log.LogTrace("Initializing state stores");

        // set initial offset limits
        updateOffsetLimits();

        foreach (IStateStore store in topology.stateStores())
{
            log.LogTrace("Initializing store {}", store.name());
            processorContext.uninitialize();
            store.init(processorContext, store);
        }
    }

    void reinitializeStateStoresForPartitions(Collection<TopicPartition> partitions)
{
        stateMgr.reinitializeStateStoresForPartitions(partitions, processorContext);
    }

    /**
     * @throws ProcessorStateException if there is an error while closing the state manager
     */
    void closeStateManager(bool clean){
        ProcessorStateException exception = null;
        log.LogTrace("Closing state manager");
        try
{

            stateMgr.close(clean);
        } catch (ProcessorStateException e)
{
            exception = e;
        } finally
{

            try
{

                stateDirectory.unlock(id);
            } catch (IOException e)
{
                if (exception == null)
{
                    exception = new ProcessorStateException(string.Format("%sFailed to release state dir lock", logPrefix), e);
                }
            }
        }
        if (exception != null)
{
            throw exception;
        }
    }

    public bool isClosed()
{
        return taskClosed;
    }

    public bool commitNeeded()
{
        return commitNeeded;
    }

    public bool hasStateStores()
{
        return !topology.stateStores().isEmpty();
    }

    public Collection<TopicPartition> changelogPartitions()
{
        return stateMgr.changelogPartitions();
    }
}