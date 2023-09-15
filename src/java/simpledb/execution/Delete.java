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
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private TransactionId transactionId;
    private OpIterator child;
    private TupleDesc countTupleDesc;

    private int callTimes=0;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.transactionId=t;
        this.child=child;
        Type[] typeAr=new Type[1];
        String[] fieldAr=new String[1];
        typeAr[0]=Type.INT_TYPE;
        fieldAr[0]="count";
        this.countTupleDesc=new TupleDesc(typeAr,fieldAr);
    }

    public TupleDesc getTupleDesc() {
        return countTupleDesc;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(callTimes!=0)
            return null;
        callTimes++;
        int count=0;
        try {
            while (child.hasNext()){
                Database.getBufferPool().deleteTuple(transactionId,child.next());
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
        child=children[0];
    }

}
