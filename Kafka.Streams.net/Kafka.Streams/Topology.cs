using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams
{
    public Topology
   
{
        /**
 * Sets the {@code auto.offset.reset} configuration when
 * {@link .AddSource(AutoOffsetReset, string, string...).Adding a source processor} or when creating {@link KStream}
 * or {@link KTable} via {@link StreamsBuilder}.
 */
        public enum AutoOffsetReset
       
{
            UNKNOWN = 0,
            EARLIEST,
            LATEST
        }
    }
}
