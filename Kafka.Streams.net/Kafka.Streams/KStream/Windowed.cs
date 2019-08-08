﻿using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.KStream
{
    public class Windowed<K>
    {
        private K key;

        private Window window;

        public Windowed(K key, Window window)
        {
            this.key = key;
            this.window = window;
        }

        public override string ToString()
        {
            return "[" + key + "@" + window.start() + "/" + window.end() + "]";
        }

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
            return window.Equals(that.window) && key.Equals(that.key);
        }

        public override int GetHashCode()
        {
            long n = ((long)window.GetHashCode() << 32) | key.GetHashCode();
            return (int)(n % 0xFFFFFFFFL);
        }
    }
}