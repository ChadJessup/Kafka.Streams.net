using Kafka.Common.Utils;
using System;
using System.Linq;
using System.Text;

namespace Kafka.Streams
{
    /**
    * Utility that handles immutable byte arrays.
    */

    //TODO: chad - 8/3/2019 - ReadOnlySpan should work here.
    public class Bytes : IComparable<Bytes>
    {
        public static LexicographicByteArrayComparator BYTES_LEXICO_COMPARATOR { get; } = new LexicographicByteArrayComparator();
        public static byte[] EMPTY { get; } = Array.Empty<byte>();

        private static readonly char[] HEX_CHARS_UPPER = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

        private readonly byte[] bytes;

        // cache the hash code for the string, default to 0
        private int hashCode;

        public static Bytes Wrap(byte[] bytes)
        {
            if (bytes == null)
            {
                return null;
            }

            return new Bytes(bytes);
        }

        /**
         * Create a Bytes using the byte array.
         *
         * @param bytes This array becomes the backing storage for the object.
         */
        public Bytes(byte[] bytes)
        {
            this.bytes = bytes;

            // initialize hash code to 0
            hashCode = 0;
        }

        /**
         * Get the data from the Bytes.
         * @return The underlying byte array
         */
        public byte[] Get() => this.bytes;

        /**
         * The hashcode is cached except for the case where it is computed as 0, in which
         * case we compute the hashcode on every call.
         *
         * @return the hashcode
         */
        public override int GetHashCode()
        {
            if (this.hashCode == 0)
            {
                unchecked
                {
                    if (bytes == null)
                    {
                        return 0;
                    }

                    var hash = 17;
                    foreach (var @byte in bytes)
                    {
                        hash = hash * 31 + @byte.GetHashCode();
                    }

                    this.hashCode = hash;
                    return hash;
                }
            }

            return this.hashCode;
        }

        public override bool Equals(object other)
        {
            if (this == other)
            {
                return true;
            }

            if (other == null)
            {
                return false;
            }

            // we intentionally use the function to compute hashcode here
            if (this.GetHashCode() != other.GetHashCode())
            {
                return false;
            }

            if (other is Bytes)
            {
                return Enumerable.SequenceEqual(this.bytes, ((Bytes)other).bytes);
            }

            return false;
        }

        public int CompareTo(Bytes that)
        {
            return 0; //BYTES_LEXICO_COMPARATOR.Compare(this.bytes, that.bytes);
        }


        public override string ToString()
        {
            return Bytes.ToString(bytes, 0, bytes.Length);
        }

        /**
         * Write a printable representation of a byte array. Non-printable
         * characters are hex escaped in the string.Format \\x%02X, eg:
         * \x00 \x05 etc.
         *
         * This function is brought from org.apache.hadoop.hbase.util.Bytes
         *
         * @param b array to write out
         * @param off offset to start at
         * @param len Length to write
         * @return string output
         */
        private static string ToString(byte[] b, int off, int len)
        {
            var result = new StringBuilder();

            if (b == null)
                return result.ToString();

            // just in case we are passed a 'len' that is > buffer Length...
            if (off >= b.Length)
                return result.ToString();

            if (off + len > b.Length)
                len = b.Length - off;

            for (var i = off; i < off + len; ++i)
            {
                var ch = b[i] & 0xFF;
                if (ch >= ' ' && ch <= '~' && ch != '\\')
                {
                    result.Append((char)ch);
                }
                else
                {
                    result.Append("\\x");
                    result.Append(HEX_CHARS_UPPER[ch / 0x10]);
                    result.Append(HEX_CHARS_UPPER[ch % 0x10]);
                }
            }

            return result.ToString();
        }

        /**
         * A byte array comparator based on lexicographic ordering.
         */
        //public static IByteArrayComparator BYTES_LEXICO_COMPARATOR = new LexicographicByteArrayComparator();
    }
}
