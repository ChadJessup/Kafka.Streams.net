using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors.Internals;
using System;
using System.IO;

namespace Kafka.Streams.KStream.Internals
{
    public class PrintForeachAction<K, V>
    {
        private readonly string label;
        private readonly PrintWriter printWriter;
        private readonly bool closable;
        private readonly IKeyValueMapper<K, V, string> mapper;

        /**
         * Print customized output with given writer. The {@link Stream} can be {@link System#out} or the others.
         *
         * @param outputStream The output stream to write to.
         * @param mapper The mapper which can allow user to customize output will be printed.
         * @param label The given name will be printed.
         */
        public PrintForeachAction(
            Stream outputStream,
            IKeyValueMapper<K, V, string> mapper,
            string label)
        {
            this.printWriter = new PrintWriter(new OutputStreamWriter(outputStream, System.Text.Encoding.UTF8));
            //this.closable = outputStream != System.out && outputStream != System.err;
            this.mapper = mapper;
            this.label = label;
        }


        public void apply(K key, V value)
        {
            var data = string.Format("[%s]: %s", label, mapper.Apply(key, value));
            //printWriter.println(data);
            if (!closable)
            {
              //  printWriter.flush();
            }
        }

        public void close()
        {
            if (closable)
            {
              //  printWriter.close();
            }
            else
            {

                //printWriter.flush();
            }
        }
    }
}
