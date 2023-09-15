package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {
    private TransactionId transactionId;

    private OpIterator child;
    private int tableId;

    private TupleDesc countTupleDesc;

    private int callTimes=0;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.transactionId=t;
        this.child=child;
        this.tableId=tableId;
        Type[] typeAr=new Type[1];
        String[] fieldAr=new String[1];
        typeAr[0]=Type.INT_TYPE;
        fieldAr[0]="count";
        this.countTupleDesc=new TupleDesc(typeAr,fieldAr);
        TupleDesc tableDesc=Database.getCatalog().getTupleDesc(tableId);
        TupleDesc childTupledesc=child.getTupleDesc();
        if(!childTupledesc.equals(tableDesc))
            throw new DbException("TupleDesc of child differs from table into which we are to insert.");
    }

    public TupleDesc getTupleDesc() {
        return this.countTupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(callTimes!=0)
            return null;
        callTimes++;
        int count=0;
        try {
            while (child.hasNext()){
                Database.getBufferPool().insertTuple(transactionId,tableId,child.next());
                count++;
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        Tuple res=new Tuple(countTupleDesc);
        res.setField(0,new IntField(count));
        return res;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child=children[0];
    }
}
