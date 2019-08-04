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
namespace Kafka.Streams.State.Internals;

using Kafka.Streams.Errors.ProcessorStateException;
using Kafka.Streams.Processor.internals.InternalProcessorContext;
















abstract class AbstractSegments<S : Segment> : Segments<S>
{
    private static Logger log = LoggerFactory.getLogger(AbstractSegments.class);

    TreeMap<long, S> segments = new TreeMap<>();
    string name;
    private long retentionPeriod;
    private long segmentInterval;
    private SimpleDateFormat formatter;

    AbstractSegments(string name, long retentionPeriod, long segmentInterval)
{
        this.name = name;
        this.segmentInterval = segmentInterval;
        this.retentionPeriod = retentionPeriod;
        // Create a date formatter. Formatted timestamps are used as segment name suffixes
        this.formatter = new SimpleDateFormat("yyyyMMddHHmm");
        this.formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));
    }

    public override long segmentId(long timestamp)
{
        return timestamp / segmentInterval;
    }

    public override string segmentName(long segmentId)
{
        // (1) previous format used - as a separator so if this changes in the future
        // then we should use something different.
        // (2) previous format used : as a separator (which did break KafkaStreams on Windows OS)
        // so if this changes in the future then we should use something different.
        return name + "." + segmentId * segmentInterval;
    }

    public override S getSegmentForTimestamp(long timestamp)
{
        return segments[segmentId(timestamp));
    }

    public override S getOrCreateSegmentIfLive(long segmentId,
                                      InternalProcessorContext context,
                                      long streamTime)
{
        long minLiveTimestamp = streamTime - retentionPeriod;
        long minLiveSegment = segmentId(minLiveTimestamp);

        S toReturn;
        if (segmentId >= minLiveSegment)
{
            // The segment is live. get it, ensure it's open, and return it.
            toReturn = getOrCreateSegment(segmentId, context);
        } else
{
            toReturn = null;
        }

        cleanupEarlierThan(minLiveSegment);
        return toReturn;
    }

    public override void openExisting(InternalProcessorContext context, long streamTime)
{
        try
{
            File dir = new File(context.stateDir(), name);
            if (dir.exists())
{
                string[] list = dir.list();
                if (list != null)
{
                    long[] segmentIds = new long[list.Length];
                    for (int i = 0; i < list.Length; i++)
{
                        segmentIds[i] = segmentIdFromSegmentName(list[i], dir);
                    }

                    // open segments in the id order
                    Arrays.sort(segmentIds);
                    foreach (long segmentId in segmentIds)
{
                        if (segmentId >= 0)
{
                            getOrCreateSegment(segmentId, context);
                        }
                    }
                }
            } else
{
                if (!dir.mkdir())
{
                    throw new ProcessorStateException(string.Format("dir %s doesn't exist and cannot be created for segments %s", dir, name));
                }
            }
        } catch (Exception ex)
{
            // ignore
        }

        long minLiveSegment = segmentId(streamTime - retentionPeriod);
        cleanupEarlierThan(minLiveSegment);
    }

    public override List<S> segments(long timeFrom, long timeTo)
{
        List<S> result = new List<>();
        NavigableMap<long, S> segmentsInRange = segments.subMap(
            segmentId(timeFrom), true,
            segmentId(timeTo), true
        );
        foreach (S segment in segmentsInRange.values())
{
            if (segment.isOpen())
{
                result.Add(segment);
            }
        }
        return result;
    }

    public override List<S> allSegments()
{
        List<S> result = new List<>();
        foreach (S segment in segments.values())
{
            if (segment.isOpen())
{
                result.Add(segment);
            }
        }
        return result;
    }

    public override void flush()
{
        foreach (S segment in segments.values())
{
            segment.flush();
        }
    }

    public override void close()
{
        foreach (S segment in segments.values())
{
            segment.close();
        }
        segments.clear();
    }

    private void cleanupEarlierThan(long minLiveSegment)
{
        Iterator<Map.Entry<long, S>> toRemove =
            segments.headMap(minLiveSegment, false).entrySet().iterator();

        while (toRemove.hasNext())
{
            Map.Entry<long, S> next = toRemove.next();
            toRemove.Remove();
            S segment = next.getValue();
            segment.close();
            try
{
                segment.destroy();
            } catch (IOException e)
{
                log.LogError("Error destroying {}", segment, e);
            }
        }
    }

    private long segmentIdFromSegmentName(string segmentName,
                                          File parent)
{
        int segmentSeparatorIndex = name.Length;
        char segmentSeparator = segmentName.charAt(segmentSeparatorIndex);
        string segmentIdString = segmentName.substring(segmentSeparatorIndex + 1);
        long segmentId;

        // old style segment name with date
        if (segmentSeparator == '-')
{
            try
{
                segmentId = formatter.parse(segmentIdString).getTime() / segmentInterval;
            } catch (ParseException e)
{
                log.LogWarning("Unable to parse segmentName {} to a date. This segment will be skipped", segmentName);
                return -1L;
            }
            renameSegmentFile(parent, segmentName, segmentId);
        } else
{
            // for both new formats (with : or .) parse segment ID identically
            try
{
                segmentId = long.parseLong(segmentIdString) / segmentInterval;
            } catch (NumberFormatException e)
{
                throw new ProcessorStateException("Unable to parse segment id as long from segmentName: " + segmentName);
            }

            // intermediate segment name with : breaks KafkaStreams on Windows OS -> rename segment file to new name with .
            if (segmentSeparator == ':')
{
                renameSegmentFile(parent, segmentName, segmentId);
            }
        }

        return segmentId;

    }

    private void renameSegmentFile(File parent,
                                   string segmentName,
                                   long segmentId)
{
        File newName = new File(parent, segmentName(segmentId));
        File oldName = new File(parent, segmentName);
        if (!oldName.renameTo(newName))
{
            throw new ProcessorStateException("Unable to rename old style segment from: "
                + oldName
                + " to new name: "
                + newName);
        }
    }

}
