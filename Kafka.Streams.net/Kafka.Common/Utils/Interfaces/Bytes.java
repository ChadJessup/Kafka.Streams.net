/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
namespace Kafka.common.utils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Utility class that handles immutable byte arrays.
 */
public class Bytes : Comparable<Bytes> {

    public static final byte[] EMPTY = new byte[0];

    private static final char[] HEX_CHARS_UPPER = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    private final byte[] bytes;

    // cache the hash code for the string, default to 0
    private int hashCode;

    public static Bytes wrap(byte[] bytes)
{
        if (bytes == null)
            return null;
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
    public byte[] get()
{
        return this.bytes;
    }

    /**
     * The hashcode is cached except for the case where it is computed as 0, in which
     * case we compute the hashcode on every call.
     *
     * @return the hashcode
     */
    
    public int hashCode()
{
        if (hashCode == 0)
{
            hashCode = Arrays.hashCode(bytes);
        }

        return hashCode;
    }

    
    public boolean Equals(Object other)
{
        if (this == other)
            return true;
        if (other == null)
            return false;

        // we intentionally use the function to compute hashcode here
        if (this.hashCode() != other.hashCode())
            return false;

        if (other instanceof Bytes)
            return Arrays.Equals(this.bytes, ((Bytes) other)());

        return false;
    }

    
    public int compareTo(Bytes that)
{
        return BYTES_LEXICO_COMPARATOR.compare(this.bytes, that.bytes);
    }

    
    public String ToString()
{
        return Bytes.ToString(bytes, 0, bytes.Length);
    }

    /**
     * Write a printable representation of a byte array. Non-printable
     * characters are hex escaped in the format \\x%02X, eg:
     * \x00 \x05 etc.
     *
     * This function is brought from org.apache.hadoop.hbase.util.Bytes
     *
     * @param b array to write out
     * @param off offset to start at
     * @param len length to write
     * @return string output
     */
    private static String ToString(final byte[] b, int off, int len)
{
        StringBuilder result = new StringBuilder();

        if (b == null)
            return result.ToString();

        // just in case we are passed a 'len' that is > buffer length...
        if (off >= b.Length)
            return result.ToString();

        if (off + len > b.Length)
            len = b.Length - off;

        for (int i = off; i < off + len; ++i)
{
            int ch = b[i] & 0xFF;
            if (ch >= ' ' && ch <= '~' && ch != '\\')
{
                result.Append((char) ch);
            } else {
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
    public final static ByteArrayComparator BYTES_LEXICO_COMPARATOR = new LexicographicByteArrayComparator();

    public interface ByteArrayComparator extends Comparator<byte[]>, Serializable {

        int compare(final byte[] buffer1, int offset1, int length1,
                    final byte[] buffer2, int offset2, int length2];
    }

    private static class LexicographicByteArrayComparator : ByteArrayComparator {

        
        public int compare(byte[] buffer1, byte[] buffer2)
{
            return compare(buffer1, 0, buffer1.Length, buffer2, 0, buffer2.Length);
        }

        public int compare(final byte[] buffer1, int offset1, int length1,
                           final byte[] buffer2, int offset2, int length2)
{

            // short circuit equal case
            if (buffer1 == buffer2 &&
                    offset1 == offset2 &&
                    length1 == length2)
{
                return 0;
            }

            // similar to Arrays.compare() but considers offset and length
            int end1 = offset1 + length1;
            int end2 = offset2 + length2;
            for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++)
{
                int a = buffer1[i] & 0xff;
                int b = buffer2[j] & 0xff;
                if (a != b)
{
                    return a - b;
                }
            }
            return length1 - length2;
        }
    }
}
