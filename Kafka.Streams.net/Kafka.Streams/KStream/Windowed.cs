using System;
using System.Diagnostics.CodeAnalysis;

namespace Kafka.Streams.KStream
{
    public class Windowed<K>
    {
        public K Key { get; private set; }
        public Window window { get; private set; }

        public Windowed(K key, Window window)
        {
            this.Key = key ?? throw new ArgumentNullException(nameof(key));
            this.window = window ?? throw new ArgumentNullException(nameof(key));
        }

        public override string ToString()
            => $"[{Key}@{window.Start()}/{window.End()}]";

        public override bool Equals(object obj)
        {
            if (obj == this)
            {
                return true;
            }

            if (!(obj is Windowed<K>))
            {
                return false;
            }

            Windowed<K> that = (Windowed<K>)obj;
            return window.Equals(that.window) && Key.Equals(that.Key);
        }

        public override int GetHashCode()
        {
            long n = ((long)window.GetHashCode() << 32) | Key.GetHashCode();
            return (int)(n % 0xFFFFFFFFL);
        }

        public K ToK()
        {
            throw new System.NotImplementedException();
        }
    }
}