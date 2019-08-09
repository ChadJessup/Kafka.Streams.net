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


using Kafka.Common.TopicPartition;
using Kafka.Common.metrics.Sensor;
using Kafka.Common.Utils.LogContext;









/**
 * RecordQueue is a FIFO queue of {@link StampedRecord} (ConsumeResult + timestamp). It also keeps track of the
 * partition timestamp defined as the largest timestamp seen on the partition so far; this is passed to the
 * timestamp extractor.
 */
public class RecordQueue
{


    public static long UNKNOWN = ConsumeResult.NO_TIMESTAMP;

    private ILogger log;
    private SourceNode source;
    private TopicPartition partition;
    private IProcessorContext<K, V> processorContext;
    private ITimestampExtractor timestampExtractor;
    private RecordDeserializer recordDeserializer;
    private ArrayDeque<ConsumeResult<byte[], byte[]>> fifoQueue;

    private StampedRecord headRecord = null;
    private long partitionTime = RecordQueue.UNKNOWN;

    private Sensor skipRecordsSensor;

    RecordQueue(TopicPartition partition,
                SourceNode source,
                ITimestampExtractor timestampExtractor,
                IDeserializationExceptionHandler deserializationExceptionHandler,
                IInternalProcessorContext<K, V>  processorContext,
                LogContext logContext)
{
        this.source = source;
        this.partition = partition;
        this.fifoQueue = new ArrayDeque<>();
        this.timestampExtractor = timestampExtractor;
        this.processorContext = processorContext;
        skipRecordsSensor = ThreadMetrics.skipRecordSensor(processorContext.metrics());
        recordDeserializer = new RecordDeserializer(
            source,
            deserializationExceptionHandler,
            logContext,
            skipRecordsSensor
        );
        this.log = logContext.logger<RecordQueue>();
    }

    /**
     * Returns the corresponding source node in the topology
     *
     * @return SourceNode
     */
    public SourceNode source()
{
        return source;
    }

    /**
     * Returns the partition with which this queue is associated
     *
     * @return TopicPartition
     */
    public TopicPartition partition()
{
        return partition;
    }

    /**
     * Add a batch of {@link ConsumeResult} into the queue
     *
     * @param rawRecords the raw records
     * @return the size of this queue
     */
    int.AddRawRecords(IEnumerable<ConsumeResult<byte[], byte[]>> rawRecords)
{
        foreach (ConsumeResult<byte[], byte[]> rawRecord in rawRecords)
{
            fifoQueue.AddLast(rawRecord);
        }

        updateHead();

        return size();
    }

    /**
     * Get the next {@link StampedRecord} from the queue
     *
     * @return StampedRecord
     */
    public StampedRecord poll()
{
        StampedRecord recordToReturn = headRecord;
        headRecord = null;

        updateHead();

        return recordToReturn;
    }

    /**
     * Returns the number of records in the queue
     *
     * @return the number of records
     */
    public int size()
{
        // plus one deserialized head record for timestamp tracking
        return fifoQueue.size() + (headRecord == null ? 0 : 1);
    }

    /**
     * Tests if the queue is empty
     *
     * @return true if the queue is empty, otherwise false
     */
    public bool isEmpty()
{
        return fifoQueue.isEmpty() && headRecord == null;
    }

    /**
     * Returns the head record's timestamp
     *
     * @return timestamp
     */
    public long headRecordTimestamp()
{
        return headRecord == null ? UNKNOWN : headRecord.timestamp;
    }

    /**
     * Returns the tracked partition time
     *
     * @return partition time
     */
    long partitionTime()
{
        return partitionTime;
    }

    /**
     * Clear the fifo queue of its elements, also clear the time tracker's kept stamped elements
     */
    public void clear()
{
        fifoQueue.clear();
        headRecord = null;
        partitionTime = RecordQueue.UNKNOWN;
    }

    private void updateHead()
{
        while (headRecord == null && !fifoQueue.isEmpty())
{
            ConsumeResult<byte[], byte[]> raw = fifoQueue.pollFirst();
            ConsumeResult<object, object> deserialized = recordDeserializer.Deserialize(processorContext, raw);

            if (deserialized == null)
{
                // this only happens if the deserializer decides to skip. It has already logged the reason.
                continue;
            }

            long timestamp;
            try
{

                timestamp = timestampExtractor.extract(deserialized, partitionTime);
            } catch (StreamsException internalFatalExtractorException)
{
                throw internalFatalExtractorException;
            } catch (Exception fatalUserException)
{
                throw new StreamsException(
                        string.Format("Fatal user code error in ITimestampExtractor callback for record %s.", deserialized),
                        fatalUserException);
            }
            log.LogTrace("Source node {} extracted timestamp {} for record {}", source.name(), timestamp, deserialized);

            // drop message if TS is invalid, i.e., negative
            if (timestamp < 0)
{
                log.LogWarning(
                        "Skipping record due to negative extracted timestamp. topic=[{}] partition=[{}] offset=[{}] extractedTimestamp=[{}] extractor=[{}]",
                        deserialized.Topic, deserialized.partition(), deserialized.offset(), timestamp, timestampExtractor.GetType().getCanonicalName()
                );

                skipRecordsSensor.record();
                continue;
            }

            headRecord = new StampedRecord(deserialized, timestamp);

            partitionTime = Math.Max(partitionTime, timestamp);
        }
    }
}
