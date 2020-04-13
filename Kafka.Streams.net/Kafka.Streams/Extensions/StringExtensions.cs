using System;
using System.Collections.Generic;
using System.Text;

namespace System
{
    public static class StringExtensions
    {
        public static byte[] GetBytes(this string value)
            => UTF8Encoding.UTF8.GetBytes(value);
    }
}
