package simpledb.systemtest;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.execution.Delete;
import simpledb.execution.Insert;
import simpledb.execution.Query;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import static org.junit.Assert.*;

/**
 * lab4 exercise3、exercise4
 * Tests running concurrent transactions.
 * You do not need to pass this test until lab3.
 */
public class TransactionTest extends SimpleDbTestBase {
    // Wait up to 10 minutes for the test to complete
    private static final int TIMEOUT_MILLIS = 10 * 60 * 1000;
    private void validateTransactions(int threads)
            throws DbException, TransactionAbortedException, IOException {
        // Create a table with a single integer value = 0
        Map<Integer, Integer> columnSpecification = new HashMap<>();
        columnSpecification.put(0, 0);
        DbFile table = SystemTestUtil.createRandomHeapFile(1, 1, columnSpecification, null);

        ModifiableCyclicBarrier latch = new ModifiableCyclicBarrier(threads);
        XactionTester[] list = new XactionTester[threads];
        for(int i = 0; i < list.length; i++) {
            list[i] = new XactionTester(table.getId(), latch);
            list[i].start();
        }

        long stopTestTime = System.currentTimeMillis() + TIMEOUT_MILLIS;
        for (XactionTester tester : list) {
            long timeout = stopTestTime - System.currentTimeMillis();
            if (timeout <= 0) {
                fail("Timed out waiting for transaction to complete");
            }
            try {
                tester.join(timeout);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (tester.isAlive()) {
                fail("Timed out waiting for transaction to complete");
            }

            if (tester.exception != null) {
                // Rethrow any exception from a child thread
                assert tester.exception != null;
                throw new RuntimeException("Child thread threw an exception.", tester.exception);
            }
            assert tester.completed;
        }

        // Check that the table has the correct value
        TransactionId tid = new TransactionId();
        DbFileIterator it = table.iterator(tid);
        it.open();
        Tuple tup = it.next();
        boolean b=it.hasNext();
        assertFalse(b);
        assertEquals(threads, ((IntField) tup.getField(0)).getValue());
        it.close();
        Database.getBufferPool().transactionComplete(tid);
        Database.getBufferPool().flushAllPages();
    }

    private static class XactionTester extends Thread {
        private final int tableId;
        private final ModifiableCyclicBarrier latch;
        public Exception exception = null;
        public boolean completed = false;

        public XactionTester(int tableId, ModifiableCyclicBarrier latch) {
            this.tableId = tableId;
            this.latch = latch;
        }

        public void run() {
            try {
                // Try to increment the value until we manage to successfully commit
                while (true) {
                    // Wait for all threads to be ready
                    latch.await();
                    Transaction tr = new Transaction();
                    try {
                        tr.start();
                        SeqScan ss1 = new SeqScan(tr.getId(), tableId, "");
                        SeqScan ss2 = new SeqScan(tr.getId(), tableId, "");

                        // read the value out of the table
                        Query q1 = new Query(ss1, tr.getId());
                        q1.start();
                        // ！注意 一个事务试图去读tableId对应表中的tuple
                        Tuple tup = q1.next();
                        IntField intf = (IntField) tup.getField(0);
                        int i = intf.getValue();
                        // 添加输出用于测试
                        System.out.println("事务开始阶段：事务"+tr.getId().getId()+" i的值为："+i);

                        // create a Tuple so that Insert can insert this new value
                        // into the table.
                        Tuple t = new Tuple(SystemTestUtil.SINGLE_INT_DESCRIPTOR);
                        // ！注意，这个地方非常容易出现问题，
                        t.setField(0, new IntField(i+1));


                        // sleep to get some interesting thread interleavings
                        // 这里sleep的目的是让多个事务共同获得一个页面的读锁，如果不sleep可能第一个事务获得读锁并趁机（还没有新的事务共享同一个读锁）将读锁升级为写锁，正常执行
                        Thread.sleep(1);

                        // race the other threads to finish the transaction: one will win
                        q1.close();

                        // delete old values (i.e., just one row) from table
                        Delete delOp = new Delete(tr.getId(), ss2);

                        Query q2 = new Query(delOp, tr.getId());
                        // 执行删除任务的时候，需要申请读锁，但是当前页面上已经存在一个读锁，且多个事务共同持有，所以不能直接将读锁升级为写锁
                        // 只能其中一个释放读锁，另外一个才能获得锁并继续执行
                        q2.start();
                        q2.next();
                        q2.close();

                        // 添加用于测试
                        SeqScan ss3 = new SeqScan(tr.getId(), tableId, "");
                        Query q4 = new Query(ss3, tr.getId());
                        q4.start();
                        boolean b=q4.hasNext();
                        //Tuple tup4 = q4.next();
                        Database.getLockManager().printLockManagerMap(tr.getId(),"test");
                        assertFalse(b);

                        // set up a Set with a tuple that is one higher than the old one.
                        Set<Tuple> hs = new HashSet<>();
                        hs.add(t);
                        TupleIterator ti = new TupleIterator(t.getTupleDesc(), hs);

                        // insert this new tuple into the table
                        Insert insOp = new Insert(tr.getId(), ti, tableId);
                        Query q3 = new Query(insOp, tr.getId());
                        q3.start();
                        q3.next();
                        q3.close();
                        // 添加用于测试
                        ss3 = new SeqScan(tr.getId(), tableId, "");
                        q4 = new Query(ss3, tr.getId());
                        q4.start();
                        Tuple tup4 = q4.next();
                        IntField intf4 = (IntField) tup4.getField(0);
                        i = intf4.getValue();
                        System.out.println("事务结束阶段：事务"+tr.getId().getId()+" i的值为："+i);

                        tr.commit();
                        break;
                    } catch (TransactionAbortedException te) {// 如果某个事务因为死锁而抛出了TransactionAbortedException，那么这个事务要执行的内容会重新执行一次（新建一个事务执行相同的内容）
                        //System.out.println("thread " + tr.getId() + " killed");
                        // give someone else a chance: abort the transaction
                        tr.transactionComplete(true);
                        latch.stillParticipating();
                    }
                }
                //System.out.println("thread " + id + " done");
            } catch (Exception e) {
                // Store exception for the master thread to handle
                exception = e;
            }
            
            try {
                latch.notParticipating();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
            completed = true;
        }
    }
    
    private static class ModifiableCyclicBarrier {
        private CountDownLatch awaitLatch;
        private CyclicBarrier participationLatch;
        private AtomicInteger nextParticipants;
        
        public ModifiableCyclicBarrier(int parties) {
            reset(parties);
        }
        
        private void reset(int parties) {
            nextParticipants = new AtomicInteger(0);
            awaitLatch = new CountDownLatch(parties);
            participationLatch = new CyclicBarrier(parties, new UpdateLatch(this, nextParticipants));
        }
        
        public void await() throws InterruptedException {
            awaitLatch.countDown();
            awaitLatch.await();
        }

        public void notParticipating() throws InterruptedException, BrokenBarrierException {
            participationLatch.await();
        }

        public void stillParticipating() throws InterruptedException, BrokenBarrierException {
            nextParticipants.incrementAndGet();
            participationLatch.await();
        }

        private static class UpdateLatch implements Runnable {
            final ModifiableCyclicBarrier latch;
            final AtomicInteger nextParticipants;
            
            public UpdateLatch(ModifiableCyclicBarrier latch, AtomicInteger nextParticipants) {
                this.latch = latch;
                this.nextParticipants = nextParticipants;
            }

            public void run() {
                // Reset this barrier if there are threads still running
                int participants = nextParticipants.get();
                if (participants > 0) {
                    latch.reset(participants);
                }
            }           
        }
    }
    
    @Test public void testSingleThread()
            throws IOException, DbException, TransactionAbortedException {
        validateTransactions(1);
    }

    /**
     *
     * @throws IOException
     * @throws DbException
     * @throws TransactionAbortedException
     * 与DeadLockTest不同，DeadLockTest某个事务超时之后就会失败、不会重试，这里事务失败之后会重试，所以如果锁处理的不好，运行效率可能非常低下
     * ！注意：如果LockManager中设置固定超时，这个测试要么成功（但是运行时间长达1min左右），要么失败Expected:2 Actual:1也就是说这种实现方法还是会出现！！并发错误！！
     */
    @Test public void testTwoThreads()
            throws IOException, DbException, TransactionAbortedException {
        validateTransactions(2);
    }

    /**
     *
     * @throws IOException
     * @throws DbException
     * @throws TransactionAbortedException
     * Expected :5
     * Actual   :1
     * 或者delete的时候发现要delete的tuple并不在页面上（这个错误之所以能发现，是因为之前严格按照文档、在delete的tuple并不在页面上情况发生时throw错误--lab之间有一定的连续性）
     * 有并发问题
     */
    @Test public void testFiveThreads()
            throws IOException, DbException, TransactionAbortedException {
        validateTransactions(5);
    }

    /**
     *
     * @throws IOException
     * @throws DbException
     * @throws TransactionAbortedException
     * 固定的wait时间，3min之后可能出现assert error
     * Expected :10
     * Actual   :1
     * 居然actual为1
     *
     * 随机wait时间
     * 运行3次，都是3min左右出现delete的tuple不在当前页面上的错误
     */
    @Test public void testTenThreads()
    throws IOException, DbException, TransactionAbortedException {
        validateTransactions(10);
    }

    @Test public void testAllDirtyFails()
            throws IOException, DbException, TransactionAbortedException {
        // Allocate a file with ~10 pages of data
        HeapFile f = SystemTestUtil.createRandomHeapFile(2, 512*10, null, null);
        Database.resetBufferPool(1);

        // BEGIN TRANSACTION
        Transaction t = new Transaction();
        t.start();

        // Insert a new row
        AbortEvictionTest.insertRow(f, t);

        // Scanning the table must fail because it can't evict the dirty page
        try {
            AbortEvictionTest.findMagicTuple(f, t);
            fail("Expected scan to run out of available buffer pages");
        } catch (DbException ignored) {}
        t.commit();
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(TransactionTest.class);
    }
}
