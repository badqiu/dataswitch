package com.github.dataswitch.demo;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Filter;
import org.rocksdb.LRUCache;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.OptimisticTransactionOptions;
import org.rocksdb.Options;
import org.rocksdb.RateLimiter;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.rocksdb.Statistics;
import org.rocksdb.Status;
import org.rocksdb.Transaction;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;

import com.google.common.collect.Lists;
 
public class RocksDBExample {
	private static final String DATA_ROOT_DIR = "/tmp/rocksdb_demo/";
    private static final String dbPath = DATA_ROOT_DIR + "/data/";
    private static final String cfdbPath = DATA_ROOT_DIR + "/data-cf/";
    private static final String txdbPath = DATA_ROOT_DIR + "/data-tx/";
 
    public static void main(String[] args) {
    	new File(DATA_ROOT_DIR).mkdirs();
    	
        testBasicOperate();
        testCustomColumnFamily();
        testTransaction();
    }
 
    public static void testBasicOperate() {
        System.out.println("开始测试rocksdb的基本操作...");
        final Options options = new Options();
        final Filter bloomFilter = new BloomFilter(10);
        final ReadOptions readOptions = new ReadOptions().setFillCache(false);
        final Statistics stats = new Statistics();
        final RateLimiter rateLimiter = new RateLimiter(10000000, 10000, 10);
 
        options.setCreateIfMissing(true)
                .setStatistics(stats)
                .setWriteBufferSize(8 * SizeUnit.KB)
                .setMaxWriteBufferNumber(3)
                .setMaxBackgroundJobs(10)
                .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                .setCompactionStyle(CompactionStyle.UNIVERSAL);
 
        final BlockBasedTableConfig table_options = new BlockBasedTableConfig();
        Cache cache = new LRUCache(64 * 1024, 6);
        table_options.setBlockCache(cache)
                .setFilterPolicy(bloomFilter)
                .setBlockSizeDeviation(5)
                .setBlockRestartInterval(10)
                .setCacheIndexAndFilterBlocks(true)
                .setBlockCacheCompressed(new LRUCache(64 * 1000, 10));
        options.setTableFormatConfig(table_options);
        options.setRateLimiter(rateLimiter);
 
        try (final RocksDB db = RocksDB.open(options, dbPath)) {
            List<byte[]> keys = Lists.newArrayList();
            keys.add("hello".getBytes());
 
            db.put("hello".getBytes(), "world".getBytes());
            byte[] value = db.get("hello".getBytes());
            System.out.format("Get('hello') = %s\n", new String(value));
 
            // write batch test
            try (final WriteOptions writeOpt = new WriteOptions()) {
                for (int i = 1; i <= 9; ++i) {
                    try (final WriteBatch batch = new WriteBatch()) {
                        for (int j = 1; j <= 9; ++j) {
                            batch.put(String.format("%dx%d", i, j).getBytes(),
                                    String.format("%d", i * j).getBytes());
                            keys.add(String.format("%dx%d", i, j).getBytes());
                        }
                        db.write(writeOpt, batch);
                    }
                }
            }
 
            System.out.println("multiGetAsList方法获取");
            List<byte[]> values = db.multiGetAsList(keys);
            for (int i = 0; i < keys.size(); i++) {
                System.out.println(String.format("key:%s,value:%s",
                        new String(keys.get(i)),
                        (values.get(i) != null ? new String(values.get(i)) : null)));
            }
 
            System.out.println("newIterator方法获取");
            RocksIterator iter = db.newIterator();
            for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                System.out.println(String.format("key:%s,value:%s",
                        new String(iter.key()), new String(iter.value())));
            }
 
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
 
    // 使用特定的列族打开数据库，可以把列族理解为关系型数据库中的表(table)
    public static void testCustomColumnFamily() {
        System.out.println("测试自定义的列簇...");
        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeLevelStyleCompaction()) {
            String cfName = "cf";
            // list of column family descriptors, first entry must always be default column family
            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                    new ColumnFamilyDescriptor(cfName.getBytes(), cfOpts)
            );
 
            List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
            try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
                 final RocksDB db = RocksDB.open(dbOptions, cfdbPath, cfDescriptors, cfHandles)) {
 
                ColumnFamilyHandle cfHandle = cfHandles.stream().filter(x -> {
                    try {
                        return (new String(x.getName())).equals(cfName);
                    } catch (RocksDBException e) {
                        return false;
                    }
                }).collect(Collectors.toList()).get(0);
 
                try {
                    // put and get from non-default column family
                    db.put(cfHandles.get(1), new WriteOptions(), "key".getBytes(), "value".getBytes());
 
                    // atomic write
                    try (final WriteBatch wb = new WriteBatch()) {
                        wb.put(cfHandles.get(0), "key2".getBytes(),
                                "value2".getBytes());
                        wb.put(cfHandles.get(1), "key3".getBytes(),
                                "value3".getBytes());
//                        wb.delete(cfHandles.get(1), "key".getBytes());
                        db.write(new WriteOptions(), wb);
                    }
 
                    System.out.println("newIterator方法获取");
                    //如果不传columnFamilyHandle，则获取默认的列簇，如果传了columnFamilyHandle，则获取指定列簇的
                    RocksIterator iter = db.newIterator(cfHandles.get(1));
                    for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                        System.out.println(String.format("key:%s,value:%s",
                                new String(iter.key()), new String(iter.value())));
                    }
 
                    // drop column family
                    db.dropColumnFamily(cfHandles.get(1));
 
                } finally {
                    for (final ColumnFamilyHandle handle : cfHandles) {
                        handle.close();
                    }
                }
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
 
    public static void testTransaction() {
        System.out.println("测试事务开始...");
        try (final Options options = new Options()
                .setCreateIfMissing(true);
             final OptimisticTransactionDB txnDb =
                     OptimisticTransactionDB.open(options, txdbPath)) {
 
            try (final WriteOptions writeOptions = new WriteOptions();
                 final ReadOptions readOptions = new ReadOptions()) {
 
                System.out.println("=========================================");
                System.out.println("Demonstrates \"Read Committed\" isolation");
                readCommitted(txnDb, writeOptions, readOptions);
                iteratorReadData(txnDb);
 
                System.out.println("=========================================");
                System.out.println("Demonstrates \"Repeatable Read\" (Snapshot Isolation) isolation");
                repeatableRead(txnDb, writeOptions, readOptions);
                iteratorReadData(txnDb);
 
                System.out.println("=========================================");
                System.out.println("Demonstrates \"Read Committed\" (Monotonic Atomic Views) isolation");
                readCommitted_monotonicAtomicViews(txnDb, writeOptions, readOptions);
                iteratorReadData(txnDb);
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
 
    private static void iteratorReadData(RocksDB db){
        System.out.println("newIterator方法获取");
        RocksIterator iter = db.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println(String.format("key:%s,value:%s",
                    new String(iter.key()), new String(iter.value())));
        }
    }
 
    /**
     * Demonstrates "Read Committed" isolation
     */
    private static void readCommitted(final OptimisticTransactionDB txnDb,
                                      final WriteOptions writeOptions, final ReadOptions readOptions)
            throws RocksDBException {
        final byte key1[] = "abc".getBytes(UTF_8);
        final byte value1[] = "def".getBytes(UTF_8);
 
        final byte key2[] = "xyz".getBytes(UTF_8);
        final byte value2[] = "zzz".getBytes(UTF_8);
 
        // Start a transaction
        try(final Transaction txn = txnDb.beginTransaction(writeOptions)) {
            // Read a key in this transaction
            byte[] value = txn.get(readOptions, key1);
            assert(value == null);
 
            // Write a key in this transaction
            txn.put(key1, value1);
 
            // Read a key OUTSIDE this transaction. Does not affect txn.
            value = txnDb.get(readOptions, key1);
            assert(value == null);
 
            // Write a key OUTSIDE of this transaction.
            // Does not affect txn since this is an unrelated key.
            // If we wrote key 'abc' here, the transaction would fail to commit.
            txnDb.put(writeOptions, key2, value2);
 
            // Commit transaction
            txn.commit();
        }
    }
    /**
     * Demonstrates "Repeatable Read" (Snapshot Isolation) isolation
     */
    private static void repeatableRead(final OptimisticTransactionDB txnDb,
                                       final WriteOptions writeOptions, final ReadOptions readOptions)
            throws RocksDBException {
 
        final byte key1[] = "ghi".getBytes(UTF_8);
        final byte value1[] = "jkl".getBytes(UTF_8);
 
        // Set a snapshot at start of transaction by setting setSnapshot(true)
        try(final OptimisticTransactionOptions txnOptions =
                    new OptimisticTransactionOptions().setSetSnapshot(true);
            final Transaction txn =
                    txnDb.beginTransaction(writeOptions, txnOptions)) {
 
            final Snapshot snapshot = txn.getSnapshot();
 
            // Write a key OUTSIDE of transaction
            txnDb.put(writeOptions, key1, value1);
 
            // Read a key using the snapshot.
            readOptions.setSnapshot(snapshot);
            final byte[] value = txn.getForUpdate(readOptions, key1, true);
            assert (value == null);
 
            try {
                // Attempt to commit transaction
                txn.commit();
                throw new IllegalStateException();
            } catch(final RocksDBException e) {
                // Transaction could not commit since the write outside of the txn
                // conflicted with the read!
                System.out.println(e.getStatus().getCode());
                assert(e.getStatus().getCode() == Status.Code.Busy);
            }
 
            txn.rollback();
        } finally {
            // Clear snapshot from read options since it is no longer valid
            readOptions.setSnapshot(null);
        }
    }
 
    /**
     * Demonstrates "Read Committed" (Monotonic Atomic Views) isolation
     *
     * In this example, we set the snapshot multiple times.  This is probably
     * only necessary if you have very strict isolation requirements to
     * implement.
     */
    private static void readCommitted_monotonicAtomicViews(
            final OptimisticTransactionDB txnDb, final WriteOptions writeOptions,
            final ReadOptions readOptions) throws RocksDBException {
 
        final byte keyX[] = "x".getBytes(UTF_8);
        final byte valueX[] = "x".getBytes(UTF_8);
 
        final byte keyY[] = "y".getBytes(UTF_8);
        final byte valueY[] = "y".getBytes(UTF_8);
 
        try (final OptimisticTransactionOptions txnOptions =
                     new OptimisticTransactionOptions().setSetSnapshot(true);
             final Transaction txn =
                     txnDb.beginTransaction(writeOptions, txnOptions)) {
 
            // Do some reads and writes to key "x"
            Snapshot snapshot = txnDb.getSnapshot();
            readOptions.setSnapshot(snapshot);
            byte[] value = txn.get(readOptions, keyX);
            txn.put(keyX, valueX);
 
            // Do a write outside of the transaction to key "y"
            txnDb.put(writeOptions, keyY, valueY);
 
            // Set a new snapshot in the transaction
            txn.setSnapshot();
            snapshot = txnDb.getSnapshot();
            readOptions.setSnapshot(snapshot);
 
            // Do some reads and writes to key "y"
            // Since the snapshot was advanced, the write done outside of the
            // transaction does not conflict.
            value = txn.getForUpdate(readOptions, keyY, true);
            txn.put(keyY, valueY);
 
            // Commit.  Since the snapshot was advanced, the write done outside of the
            // transaction does not prevent this transaction from Committing.
            txn.commit();
 
        } finally {
            // Clear snapshot from read options since it is no longer valid
            readOptions.setSnapshot(null);
        }
    }
}
