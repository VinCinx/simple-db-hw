package simpledb;

import simpledb.TestUtil.SkeletonFile;
import simpledb.common.Database;
import simpledb.common.Utility;
import simpledb.storage.*;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

/**
 * lab1 exercise4
 */
public class HeapPageReadTest extends SimpleDbTestBase {
    private HeapPageId pid;

    public static final int[][] EXAMPLE_VALUES = new int[][] {
        { 31933, 862 },
        { 29402, 56883 },
        { 1468, 5825 },
        { 17876, 52278 },
        { 6350, 36090 },
        { 34784, 43771 },
        { 28617, 56874 },
        { 19209, 23253 },
        { 56462, 24979 },
        { 51440, 56685 },
        { 3596, 62307 },
        { 45569, 2719 },
        { 22064, 43575 },
        { 42812, 44947 },
        { 22189, 19724 },
        { 33549, 36554 },
        { 9086, 53184 },
        { 42878, 33394 },
        { 62778, 21122 },
        { 17197, 16388 }
    };

    public static final byte[] EXAMPLE_DATA;
    static {
        // Build the input table
        List<List<Integer>> table = new ArrayList<>();
        for (int[] tuple : EXAMPLE_VALUES) {
            List<Integer> listTuple = new ArrayList<>();
            for (int value : tuple) {
                listTuple.add(value);
            }
            table.add(listTuple);
        }

        // Convert it to a HeapFile and read in the bytes
        try {
            File temp = File.createTempFile("table", ".dat");
            temp.deleteOnExit();
            HeapFileEncoder.convert(table, temp, BufferPool.getPageSize(), 2);
            EXAMPLE_DATA = TestUtil.readFileBytes(temp.getAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Set up initial resources for each unit test.
     */
    @Before public void addTable() {
        this.pid = new HeapPageId(-1, -1);
        Database.getCatalog().addTable(new SkeletonFile(-1, Utility.getTupleDesc(2)), SystemTestUtil.getUUID());
    }

    /**
     * Unit test for HeapPage.getId()
     */
    @Test public void getId() throws Exception {
        HeapPage page = new HeapPage(pid, EXAMPLE_DATA);
        assertEquals(pid, page.getId());
    }

    /**
     * Unit test for HeapPage.iterator()
     */
    @Test public void testIterator() throws Exception {
        HeapPage page = new HeapPage(pid, EXAMPLE_DATA);
        Iterator<Tuple> it = page.iterator();

        int row = 0;
        while (it.hasNext()) {
            Tuple tup = it.next();
            IntField f0 = (IntField) tup.getField(0);
            IntField f1 = (IntField) tup.getField(1);

            assertEquals(EXAMPLE_VALUES[row][0], f0.getValue());
            assertEquals(EXAMPLE_VALUES[row][1], f1.getValue());
            row++;
        }
        assertEquals(row, EXAMPLE_VALUES.length);
        /**
         * 这个测试用例非常不完备，可能导致代码为之后的实验留坑
         * 1.只要EXAMPLE_VALUES够长，EXAMPLE_DATA可能包含不只一个页面，此测试用例默认一个页面了
         * 2.EXAMPLE_DATA中，每个页面中的slot都是顺序占用的，有效的tuple相邻，testIterator()没有考虑删除某个中间的tuple的情况——这就导致我一开始对于page的testIterator()只是简单截取了page的tuple[]的前面连续的一部分，截取使用的长度为已经占用的slot，测试也能通过，实际上这是错的。已经占用的slot不一定连续分布在在tuple[]前面
         */
    }

    /**
     * Unit test for HeapPage.getNumEmptySlots()
     */
    @Test public void getNumEmptySlots() throws Exception {
        HeapPage page = new HeapPage(pid, EXAMPLE_DATA);
        assertEquals(484, page.getNumEmptySlots());
    }

    /**
     * Unit test for HeapPage.isSlotUsed()
     */
    @Test public void getSlot() throws Exception {
        HeapPage page = new HeapPage(pid, EXAMPLE_DATA);

        for (int i = 0; i < 20; ++i)
            assertTrue(page.isSlotUsed(i));

        for (int i = 20; i < 504; ++i)
            assertFalse(page.isSlotUsed(i));
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(HeapPageReadTest.class);
    }
}
