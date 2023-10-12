package simpledb.systemtest;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import simpledb.common.DbException;
import simpledb.execution.Delete;
import simpledb.execution.Filter;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.HeapFile;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * lab2 exercise4；完成lab4 LockManager，但是尚未完成lab4 exercise4的transactionComplete()方法时，该测试会一直转圈圈，因为该测试内调用transactionComplete()释放锁时实际上并没有释放锁，后续transaction获取锁失败一直wait()；一开始lab2能通过是因为没有加锁，也就不需要释放锁
 */
public class DeleteTest extends FilterBase {
    List<List<Integer>> expectedTuples = null;

    @Override
    protected int applyPredicate(HeapFile table, TransactionId tid, Predicate predicate)
            throws DbException, TransactionAbortedException {
        SeqScan ss = new SeqScan(tid, table.getId(), "");
        Filter filter = new Filter(predicate, ss);
        Delete deleteOperator = new Delete(tid, filter);
//        Query q = new Query(deleteOperator, tid);

//        q.start();
        deleteOperator.open();
        boolean hasResult = false;
        int result = -1;
        while (deleteOperator.hasNext()) {
            Tuple t = deleteOperator.next();
            assertFalse(hasResult);
            hasResult = true;
            assertEquals(SystemTestUtil.SINGLE_INT_DESCRIPTOR, t.getTupleDesc());
            result = ((IntField) t.getField(0)).getValue();
        }
        assertTrue(hasResult);

        deleteOperator.close();

        // As part of the same transaction, scan the table
        if (result == 0) {
            // Deleted zero tuples: all tuples still in table
            expectedTuples = createdTuples;
        } else {
            assert result == createdTuples.size();
            expectedTuples = new ArrayList<>();
        }
        SystemTestUtil.matchTuples(table, tid, expectedTuples);
        return result;
    }

    @Override
    protected void validateAfter(HeapFile table)
            throws DbException, TransactionAbortedException, IOException {
        // As part of a different transaction, scan the table
        SystemTestUtil.matchTuples(table, expectedTuples);
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(DeleteTest.class);
    }
}
