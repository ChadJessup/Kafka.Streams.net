using System;
using System.Collections.Generic;
using System.Text;

namespace System.IO
{
    public static class SystemIOExtensions
    {
        public static IEnumerable<FileInfo> listFiles(this DirectoryInfo directory)
            => directory.GetFiles();

        public static void Lock(this FileStream stream)
            => stream.Lock(0, stream.Length);
    }
}
