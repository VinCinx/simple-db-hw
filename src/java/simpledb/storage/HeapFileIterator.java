package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * HeapFileIterator独立于HeapFile，而不是让HeapFile继承或者实现，否则的话返回的就是HeapFile，其中携带TransactionId信息，并发访问有问题
 * 这里首先尝试了extends帮助类AbstractDbFileIterator，但是在测试testIteratorClose的时候需要访问父类的next，而next是private，为了不修改原有的代码，改为implements DbFileIterator
 * 在SeqScan中要使用这个类读取，所以设置为public
 */
public class HeapFileIterator implements DbFileIterator {

    private TransactionId transactionId;

    private HeapFile heapFile;

    private Tuple next = null;

    private boolean isOpen = false;

    private int nextPgNo;

    Iterator<Tuple> curPageIter;

    public HeapFileIterator(TransactionId transactionId, HeapFile heapFile) {
        this.transactionId = transactionId;
        this.heapFile = heapFile;
        this.nextPgNo = 0;
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (next == null) next = readNext();
        return next != null;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {//todo hasNext之后不调用next读取就close，下次再打开的时候从什么位置读，从测试中没有看出它的要求，貌似从测试中看出来暂时没有这个需求
        if (next == null) {
            next = readNext();
            if (next == null) throw new NoSuchElementException();
        }

        Tuple result = next;
        next = null;
        return result;
    }

    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (!isOpen) {
            return null;
        }
        boolean iterHasNext = curPageIter != null && curPageIter.hasNext();
        if (iterHasNext) {
            return curPageIter.next();
        }
        while( curPageIter==null || (!curPageIter.hasNext() && !(nextPgNo >= this.heapFile.numPages())) ) {//这里是>=，nextPgNo从0开始，=heapFile.numPages()的时候已经读完了所有的页面
            HeapPageId pid = new HeapPageId(heapFile.getId(), nextPgNo);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(transactionId, pid, Permissions.READ_ONLY);
            nextPgNo++;
            curPageIter = heapPage.iterator();
        }
        if(curPageIter.hasNext())//可能最后一个页面上一个tuple都没有，中间呢，中间也可能某个页面一个Tuple都没有，那种情况下现在的写法就有问题，这个版本修改好了
            return curPageIter.next();
        return null;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.isOpen = true;
    }

    @Override
    public void close() {
        this.isOpen = false;
        this.next = null;//重置，保证执行了hasNext()之后关闭Iterator仍然能保证正确性
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        curPageIter = null;
        nextPgNo = 0;
    }
}
