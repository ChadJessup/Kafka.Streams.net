namespace Kafka.Streams.Processors.Internals
{
    /**
     * Describe records to delete in a call to {@link Admin#deleteRecords(Map)}
     *
     * The API of this class is evolving, see {@link Admin} for details.
     */
    public class RecordsToDelete
    {
        private readonly long offset;

        private RecordsToDelete(long offset)
        {
            this.offset = offset;
        }

        /**
         * Delete All the records before the given {@code offset}
         *
         * @param offset    the offset before which All records will be deleted
         */
        public static RecordsToDelete BeforeOffset(long offset)
        {
            return new RecordsToDelete(offset);
        }

        /**
         * The offset before which All records will be deleted
         */
        public long BeforeOffset()
        {
            return this.offset;
        }

        public override bool Equals(object o)
        {
            if (this == o) return true;
            if (o == null || this.GetType() != o.GetType()) return false;

            var that = (RecordsToDelete)o;

            return this.offset == that.offset;
        }

        public override int GetHashCode()
        {
            return (int)this.offset;
        }

        public override string ToString()
        {
            return "(beforeOffset = " + this.offset + ")";
        }
    }
}