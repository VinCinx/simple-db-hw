package simpledb.systemtest;

import simpledb.common.Database;
import simpledb.common.DbException;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.junit.Test;

import simpledb.common.Utility;
import simpledb.execution.IndexPredicate;
import simpledb.execution.Predicate.Op;
import simpledb.index.BTreeFile;
import simpledb.index.BTreeScan;
import simpledb.index.BTreeUtility;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * lab5 exercise1
 * 一开始testSmall()和testReadPage()两个测试有问题，会报错throw new RuntimeException("all type lock not exist in the page, the pageI...，也就是锁莫名奇妙没了，对测试debug之后发现锁会在某个时刻突然全没了，但是TransactionId中的occupiedLocks还包含锁，也就是事务还持有锁，这是及其不合理的
 * 通过很多锁会突然全部消失的行为，估计应该是测试调用了 resetLockManager() 导致的，该函数只在resetBufferPool函数中被调用，测试中确实调用了resetBufferPool
 * resetBufferPool中调用resetLockManager()是我在完成lab4时添加的，lab4有一个测试LockingTest，该测试的setUp()函数中有一行注释，表明了希望通过resetBufferPool释放已有的锁状态，所以我在resetBufferPool中调用resetLockManager()释放所有锁
 * but，当时没有考虑到释放锁时，将持有这些锁的事务也进行更新，让所有事务不再持有锁（因为锁全部被释放了），在LockingTest中由于占用这些锁的事务最终并没有commit或者abort释放锁，所以没有报运行时错误throw new RuntimeException("all type lock not exist in the page, the pageI...
 * 不过当时没有想起将事务对应的锁释放也正常，因为resetBufferPool本就是一个专门为测试设计的方法，一般不会调用，当时只读到注释要求resetBufferPool能释放锁于是就简单添加该功能。不过这个功能确实不能在实验代码中一开始就写的完备（包含释放锁、释放事务占用的锁的功能），因为初始代码中没有添加事务和锁，这都是lab4中实现的。
 *
 * 定位到问题就很简单了，在resetLockManager()中添加代码，释放所有锁的同时，释放所有事务占用的锁，让事务回到一个锁也不占用的初始状态
 * 修改后，本测试中的4个小测试全部通过。
 *
 * Dumps the contents of a table.
 * args[1] is the number of columns.  E.g., if it's 5, then BTreeScanTest will end
 * up dumping the contents of f4.0.txt.
 */
public class BTreeScanTest extends SimpleDbTestBase {
    private final static Random r = new Random();
    
    /** Tests the scan operator for a table with the specified dimensions. */
    private void validateScan(int[] columnSizes, int[] rowSizes)
            throws IOException, DbException, TransactionAbortedException {
    	TransactionId tid = new TransactionId();
    	for (int columns : columnSizes) {
    		int keyField = r.nextInt(columns);
            for (int rows : rowSizes) {
                List<List<Integer>> tuples = new ArrayList<>();
                BTreeFile f = BTreeUtility.createRandomBTreeFile(columns, rows, null, tuples, keyField);
                BTreeScan scan = new BTreeScan(tid, f.getId(), "table", null);
                SystemTestUtil.matchTuples(scan, tuples);
                Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
            }
        }
    	Database.getBufferPool().transactionComplete(tid);
    }
    
    // comparator to sort Tuples by key field
    private static class TupleComparator implements Comparator<List<Integer>> {
        private final int keyField;
        
        public TupleComparator(int keyField) {
        	this.keyField = keyField;
        }
        
    	public int compare(List<Integer> t1, List<Integer> t2) {
            int cmp = 0;
            if(t1.get(keyField) < t2.get(keyField)) {
            	cmp = -1;
            }
            else if(t1.get(keyField) > t2.get(keyField)) {
            	cmp = 1;
            }
            return cmp;
        }
    }
    
    /** Counts the number of readPage operations. */
    static class InstrumentedBTreeFile extends BTreeFile {
        public InstrumentedBTreeFile(File f, int keyField, TupleDesc td) {
            super(f, keyField, td);
        }

        @Override
        public Page readPage(PageId pid) throws NoSuchElementException {
            readCount += 1;
            return super.readPage(pid);
        }

        public int readCount = 0;
    }
    
    /** Scan 1-4 columns. */
    @Test public void testSmall() throws IOException, DbException, TransactionAbortedException {
        int[] columnSizes = new int[]{1, 2, 3, 4};
        int[] rowSizes =
                new int[]{0, 1, 2, 511, 512, 513, 1023, 1024, 1025, 4096 + r.nextInt(4096)};
        validateScan(columnSizes, rowSizes);
    }

    /** Test that rewinding a BTreeScan iterator works. */
    @Test public void testRewind() throws IOException, DbException, TransactionAbortedException {
        List<List<Integer>> tuples = new ArrayList<>();
        int keyField = r.nextInt(2);
        BTreeFile f = BTreeUtility.createRandomBTreeFile(2, 1000, null, tuples, keyField);
        tuples.sort(new TupleComparator(keyField));
        
        TransactionId tid = new TransactionId();
        BTreeScan scan = new BTreeScan(tid, f.getId(), "table", null);
        scan.open();
        for (int i = 0; i < 100; ++i) {
            assertTrue(scan.hasNext());
            Tuple t = scan.next();
            assertEquals(tuples.get(i), SystemTestUtil.tupleToList(t));
        }

        scan.rewind();
        for (int i = 0; i < 100; ++i) {
            assertTrue(scan.hasNext());
            Tuple t = scan.next();
            assertEquals(tuples.get(i), SystemTestUtil.tupleToList(t));
        }
        scan.close();
        Database.getBufferPool().transactionComplete(tid);
    }
    
    /** Test that rewinding a BTreeScan iterator works with predicates. */
    @Test public void testRewindPredicates() throws IOException, DbException, TransactionAbortedException {
    	// Create the table
        List<List<Integer>> tuples = new ArrayList<>();
        int keyField = r.nextInt(3);
        BTreeFile f = BTreeUtility.createRandomBTreeFile(3, 1000, null, tuples, keyField);
        tuples.sort(new TupleComparator(keyField));
                
        // EQUALS
        TransactionId tid = new TransactionId();
        List<List<Integer>> tuplesFiltered = new ArrayList<>();
        IndexPredicate ipred = new IndexPredicate(Op.EQUALS, new IntField(r.nextInt(BTreeUtility.MAX_RAND_VALUE)));
        Iterator<List<Integer>> it = tuples.iterator();
        while(it.hasNext()) {
        	List<Integer> tup = it.next();
        	if(tup.get(keyField) == ((IntField) ipred.getField()).getValue()) {
        		tuplesFiltered.add(tup);
        	}
        }
        
        BTreeScan scan = new BTreeScan(tid, f.getId(), "table", ipred);
        scan.open();
        for (List<Integer> item : tuplesFiltered) {
            assertTrue(scan.hasNext());
            Tuple t = scan.next();
            assertEquals(item, SystemTestUtil.tupleToList(t));
        }

        scan.rewind();
        for (List<Integer> value : tuplesFiltered) {
            assertTrue(scan.hasNext());
            Tuple t = scan.next();
            assertEquals(value, SystemTestUtil.tupleToList(t));
        }
        scan.close();
        
        // LESS_THAN
        tuplesFiltered.clear();
        ipred = new IndexPredicate(Op.LESS_THAN, new IntField(r.nextInt(BTreeUtility.MAX_RAND_VALUE)));
        it = tuples.iterator();
        while(it.hasNext()) {
        	List<Integer> tup = it.next();
        	if(tup.get(keyField) < ((IntField) ipred.getField()).getValue()) {
        		tuplesFiltered.add(tup);
        	}
        }
        
        scan = new BTreeScan(tid, f.getId(), "table", ipred);
        scan.open();
        for (List<Integer> list : tuplesFiltered) {
            assertTrue(scan.hasNext());
            Tuple t = scan.next();
            assertEquals(list, SystemTestUtil.tupleToList(t));
        }

        scan.rewind();
        for (List<Integer> arrayList : tuplesFiltered) {
            assertTrue(scan.hasNext());
            Tuple t = scan.next();
            assertEquals(arrayList, SystemTestUtil.tupleToList(t));
        }
        scan.close();
        
        // GREATER_THAN
        tuplesFiltered.clear();
        ipred = new IndexPredicate(Op.GREATER_THAN_OR_EQ, new IntField(r.nextInt(BTreeUtility.MAX_RAND_VALUE)));
        it = tuples.iterator();
        while(it.hasNext()) {
        	List<Integer> tup = it.next();
        	if(tup.get(keyField) >= ((IntField) ipred.getField()).getValue()) {
        		tuplesFiltered.add(tup);
        	}
        }
        
        scan = new BTreeScan(tid, f.getId(), "table", ipred);
        scan.open();
        for (List<Integer> integerArrayList : tuplesFiltered) {
            assertTrue(scan.hasNext());
            Tuple t = scan.next();
            assertEquals(integerArrayList, SystemTestUtil.tupleToList(t));
        }

        scan.rewind();
        for (List<Integer> integers : tuplesFiltered) {
            assertTrue(scan.hasNext());
            Tuple t = scan.next();
            assertEquals(integers, SystemTestUtil.tupleToList(t));
        }
        scan.close();
        Database.getBufferPool().transactionComplete(tid);
    }
    
    /** Test that scanning the BTree for predicates does not read all the pages */
    @Test public void testReadPage() throws Exception {
    	// Create the table
        final int LEAF_PAGES = 30;
    	
    	List<List<Integer>> tuples = new ArrayList<>();
        int keyField = 0;
        BTreeFile f = BTreeUtility.createBTreeFile(2, LEAF_PAGES*502, null, tuples, keyField);
        tuples.sort(new TupleComparator(keyField));
        TupleDesc td = Utility.getTupleDesc(2);
        InstrumentedBTreeFile table = new InstrumentedBTreeFile(f.getFile(), keyField, td);
        Database.getCatalog().addTable(table, SystemTestUtil.getUUID());
        
        // EQUALS
        TransactionId tid = new TransactionId();
        List<List<Integer>> tuplesFiltered = new ArrayList<>();
        IndexPredicate ipred = new IndexPredicate(Op.EQUALS, new IntField(r.nextInt(LEAF_PAGES*502)));
        Iterator<List<Integer>> it = tuples.iterator();
        while(it.hasNext()) {
        	List<Integer> tup = it.next();
        	if(tup.get(keyField) == ((IntField) ipred.getField()).getValue()) {
        		tuplesFiltered.add(tup);
        	}
        }
        
        Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
        table.readCount = 0;
        BTreeScan scan = new BTreeScan(tid, f.getId(), "table", ipred);
        SystemTestUtil.matchTuples(scan, tuplesFiltered);
        // root pointer page + root + leaf page (possibly 2 leaf pages)
        assertTrue(table.readCount == 3 || table.readCount == 4);
        
        // LESS_THAN
        tuplesFiltered.clear();
        ipred = new IndexPredicate(Op.LESS_THAN, new IntField(r.nextInt(LEAF_PAGES*502)));
        it = tuples.iterator();
        while(it.hasNext()) {
        	List<Integer> tup = it.next();
        	if(tup.get(keyField) < ((IntField) ipred.getField()).getValue()) {
        		tuplesFiltered.add(tup);
        	}
        }
        
        Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
        table.readCount = 0;
        scan = new BTreeScan(tid, f.getId(), "table", ipred);
        SystemTestUtil.matchTuples(scan, tuplesFiltered);
        // root pointer page + root + leaf pages
        int leafPageCount = tuplesFiltered.size()/502;
        if(leafPageCount < LEAF_PAGES)
        	leafPageCount++; // +1 for next key locking
        assertEquals(leafPageCount + 2, table.readCount);
        
        // GREATER_THAN
        tuplesFiltered.clear();
        ipred = new IndexPredicate(Op.GREATER_THAN_OR_EQ, new IntField(r.nextInt(LEAF_PAGES*502)));
        it = tuples.iterator();
        while(it.hasNext()) {
        	List<Integer> tup = it.next();
        	if(tup.get(keyField) >= ((IntField) ipred.getField()).getValue()) {
        		tuplesFiltered.add(tup);
        	}
        }
        
        Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
        table.readCount = 0;
        scan = new BTreeScan(tid, f.getId(), "table", ipred);
        SystemTestUtil.matchTuples(scan, tuplesFiltered);
        // root pointer page + root + leaf pages
        leafPageCount = tuplesFiltered.size()/502;
        if(leafPageCount < LEAF_PAGES)
        	leafPageCount++; // +1 for next key locking
        assertEquals(leafPageCount + 2, table.readCount);
        
        Database.getBufferPool().transactionComplete(tid);
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(BTreeScanTest.class);
    }
}
