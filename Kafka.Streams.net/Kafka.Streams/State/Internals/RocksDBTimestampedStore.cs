//using Kafka.Streams.Errors;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Interfaces;
//using Microsoft.Extensions.Logging;
//using RocksDbSharp;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    /**
//     * A persistent key-(value-timestamp) store based on RocksDb.
//     */
//    public class RocksDbTimestampedStore : RocksDbStore, ITimestampedBytesStore
//    {
//        private static ILogger log = new LoggerFactory().CreateLogger<RocksDbTimestampedStore>();

//        public RocksDbTimestampedStore(string name)
//            : base(name)
//        {
//        }

//        public RocksDbTimestampedStore(string name,
//                                string parentDir)
//            : base(name, parentDir)
//        {
//        }


//        void openRocksDb(DBOptions dbOptions,
//                         ColumnFamilyOptions columnFamilyOptions)
//        {
//            List<ColumnFamilyDescriptor> columnFamilyDescriptors = asList(
//                new ColumnFamilyDescriptor(RocksDb.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
//                new ColumnFamilyDescriptor("keyValueWithTimestamp".getBytes(System.Text.Encoding.UTF8), columnFamilyOptions));
//            List<ColumnFamilyHandle> columnFamilies = new List<ColumnFamilyHandle>(columnFamilyDescriptors.size());

//            try
//            {
//                db = RocksDb.open(dbOptions, dbDir.FullName, columnFamilyDescriptors, columnFamilies);

//                ColumnFamilyHandle noTimestampColumnFamily = columnFamilies[0];

//                RocksIterator noTimestampsIter = db.newIterator(noTimestampColumnFamily);
//                noTimestampsIter.seekToFirst();
//                if (noTimestampsIter.isValid())
//                {
//                    log.LogInformation("Opening store {} in upgrade mode", name);
//                    dbAccessor = new DualColumnFamilyAccessor(noTimestampColumnFamily, columnFamilies[1]);
//                }
//                else
//                {
//                    log.LogInformation("Opening store {} in regular mode", name);
//                    dbAccessor = new SingleColumnFamilyAccessor(columnFamilies[1]);
//                    noTimestampColumnFamily.close();
//                }
//                noTimestampsIter.close();
//            }
//            catch (RocksDbException e)
//            {
//                if ("Column family not found: : keyValueWithTimestamp".Equals(e.getMessage()))
//                {
//                    try
//                    {
//                        db = RocksDb.Open(dbOptions, dbDir.FullName, columnFamilyDescriptors.subList(0, 1), columnFamilies);
//                        columnFamilies.Add(db.createColumnFamily(columnFamilyDescriptors[1]));
//                    }
//                    catch (RocksDbException fatal)
//                    {
//                        throw new ProcessorStateException("Error opening store " + name + " at location " + dbDir.ToString(), fatal);
//                    }
//                    log.LogInformation("Opening store {} in upgrade mode", name);
//                    dbAccessor = new DualColumnFamilyAccessor(columnFamilies[0], columnFamilies[1]);
//                }
//                else
//                {
//                    throw new ProcessorStateException("Error opening store " + name + " at location " + dbDir.ToString(), e);
//                }
//            }
//        }
//    }
//}