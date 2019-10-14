using Kafka.Streams.Errors;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Microsoft.Extensions.Logging;
using RocksDbSharp;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.State.Internals
{
    /**
     * A persistent key-(value-timestamp) store based on RocksDb.
     */
    public class RocksDbTimestampedStore : RocksDbStore, ITimestampedBytesStore
    {
        private static readonly ILogger log = new LoggerFactory().CreateLogger<RocksDbTimestampedStore>();

        public RocksDbTimestampedStore(string name)
            : base(name)
        {
        }

        public RocksDbTimestampedStore(string name,
                                string parentDir)
            : base(name, parentDir)
        {
        }


        void openRocksDb(
            DbOptions dbOptions,
            ColumnFamilyOptions columnFamilyOptions)
        {
            List<ColumnFamilies.Descriptor> columnFamilyDescriptors = new List<ColumnFamilies.Descriptor>
            {
                new ColumnFamilies.Descriptor(ColumnFamilies.DefaultName, columnFamilyOptions),
                new ColumnFamilies.Descriptor("keyValueWithTimestamp", columnFamilyOptions),
            };

            List<ColumnFamilyHandle> columnFamilies = new List<ColumnFamilyHandle>();

            try
            {
                Db = RocksDb.Open(
                    dbOptions,
                    DbDir.FullName);
                //                    , columnFamilyDescriptors);

                ColumnFamilyHandle noTimestampColumnFamily = columnFamilies[0];

                Iterator noTimestampsIter = Db.NewIterator(noTimestampColumnFamily);
                noTimestampsIter.SeekToFirst();
                if (noTimestampsIter.Valid())
                {
                    log.LogInformation($"Opening store {name} in upgrade mode");
                    DbAccessor = new DualColumnFamilyAccessor(
                        name,
                        Db,
                        this.WOptions,
                        noTimestampColumnFamily,
                        columnFamilies[1]);
                }
                else
                {
                    log.LogInformation($"Opening store {name} in regular mode");
                    DbAccessor = new SingleColumnFamilyAccessor(
                        name,
                        Db,
                        this.WOptions,
                        OpenIterators,
                        columnFamilies[1]);

                    //noTimestampColumnFamily.Close();
                }

                //noTimestampsIter.close();
            }
            catch (RocksDbException e)
            {
                if ("Column family not found: : keyValueWithTimestamp".Equals(e.ToString()))
                {
                    try
                    {
                        Db = RocksDb.Open(
                            dbOptions,
                            DbDir.FullName);
                        //, columnFamilyDescriptors.Take(2),
                        //columnFamilies);

                        //columnFamilies.Add(db.CreateColumnFamily(columnFamilyDescriptors[1], dbDir.Name));
                    }
                    catch (RocksDbException fatal)
                    {
                        throw new ProcessorStateException("Error opening store " + name + " at location " + DbDir.ToString(), fatal);
                    }

                    log.LogInformation("Opening store {} in upgrade mode", name);

                    DbAccessor = new DualColumnFamilyAccessor(
                        name,
                        Db,
                        WOptions,
                        columnFamilies[0],
                        columnFamilies[1]);
                }
                else
                {
                    throw new ProcessorStateException("Error opening store " + name + " at location " + DbDir.ToString(), e);
                }
            }
        }
    }
}