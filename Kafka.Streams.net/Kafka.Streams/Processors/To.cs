using System;

namespace Kafka.Streams.Processors
{
    /**
     * This is used to provide the optional parameters when sending output records to downstream processor
     * using {@link IProcessorContext#forward(object, object, To)}.
     */
    public class To
    {
        protected string childName;
        public long Timestamp { get; private set; }

        private To(string childName, long timestamp)
        {
            this.childName = childName;
            this.Timestamp = timestamp;
        }

        protected To(To to)
            : this(to.childName, to.Timestamp)
        {
        }

        public virtual void Update(To to)
        {
            this.childName = to.childName;
            this.Timestamp = to.Timestamp;
        }

        /**
         * Forward the key/value pair to one of the downstream processors designated by the downstream processor Name.
         * @param childName Name of downstream processor
         * @return a new {@link To} instance configured with {@code childName}
         */
        public static To Child(string childName)
        {
            return new To(childName, -1);
        }

        /**
         * Forward the key/value pair to All downstream processors
         * @return a new {@link To} instance configured for All downstream processor
         */
        public static To All()
        {
            return new To(null, -1);
        }

        /**
         * Set the timestamp of the output record.
         * @param timestamp the output record timestamp
         * @return itself (i.e., {@code this})
         */
        public To WithTimestamp(long timestamp)
        {
            this.Timestamp = timestamp;
            return this;
        }


        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || this.GetType() != o.GetType())
            {
                return false;
            }
            var to = (To)o;
            return this.Timestamp == to.Timestamp &&
                this.childName.Equals(to.childName);
        }

        /**
         * Equality is implemented in support of tests, *not* for use in Hash collections, since this is mutable.
         */
        public override int GetHashCode()
        {
            throw new InvalidOperationException("To is unsafe for use in Hash collections");
        }

    }
}