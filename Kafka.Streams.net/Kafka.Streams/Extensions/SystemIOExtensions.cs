using System;
using System.Collections.Generic;
using System.Text;

namespace System.IO
{
    public static class SystemIOExtensions
    {
        public static IEnumerable<FileSystemInfo> listFiles(this DirectoryInfo directory)
        {
            if (directory is null)
            {
                throw new ArgumentNullException(nameof(directory));
            }

            return directory.EnumerateFileSystemInfos("*.*", SearchOption.AllDirectories);
        }

        public static void Lock(this FileStream stream)
        {
            if (stream is null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            stream.Lock(0, stream.Length);
        }
        public static void Unlock(this FileStream stream)
        {
            if (stream is null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            stream.Unlock(0, stream.Length);
        }
    }
}
