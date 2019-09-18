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
                db = RocksDb.Open(
                    dbOptions,
                    dbDir.FullName);
                //                    , columnFamilyDescriptors);

                ColumnFamilyHandle noTimestampColumnFamily = columnFamilies[0];

                Iterator noTimestampsIter = db.NewIterator(noTimestampColumnFamily);
                noTimestampsIter.SeekToFirst();
                if (noTimestampsIter.Valid())
                {
                    log.LogInformation($"Opening store {name} in upgrade mode");
                    dbAccessor = new DualColumnFamilyAccessor(
                        name,
                        db,
                        this.wOptions,
                        noTimestampColumnFamily,
                        columnFamilies[1]);
                }
                else
                {
                    log.LogInformation($"Opening store {name} in regular mode");
                    dbAccessor = new SingleColumnFamilyAccessor(
                        name,
                        db,
                        this.wOptions,
                        openIterators,
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
                        db = RocksDb.Open(
                            dbOptions,
                            dbDir.FullName);
                        //, columnFamilyDescriptors.Take(2),
                        //columnFamilies);

                        //columnFamilies.Add(db.CreateColumnFamily(columnFamilyDescriptors[1], dbDir.Name));
                    }
                    catch (RocksDbException fatal)
                    {
                        throw new ProcessorStateException("Error opening store " + name + " at location " + dbDir.ToString(), fatal);
                    }

                    log.LogInformation("Opening store {} in upgrade mode", name);

                    dbAccessor = new DualColumnFamilyAccessor(
                        name,
                        db,
                        wOptions,
                        columnFamilies[0],
                        columnFamilies[1]);
                }
                else
                {
                    throw new ProcessorStateException("Error opening store " + name + " at location " + dbDir.ToString(), e);
                }
            }
        }
    }
}