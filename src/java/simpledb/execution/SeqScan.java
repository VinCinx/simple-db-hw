package simpledb.execution;

import simpledb.common.Database;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;

    private int tableId;

    private String tableAlias;

    private HeapFile heapFile;

    private HeapFileIterator heapFileIterator;


    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.transactionId=tid;
        this.tableId=tableid;
        this.tableAlias=tableAlias;
        heapFile=(HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.heapFileIterator=(HeapFileIterator) (heapFile).iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).；相同的事务，但是在这个事务中scan另外一张表，所以要切换到新heapfile和heapFileIterator
     */
    public void reset(int tableid, String tableAlias) {
        this.tableId=tableid;
        this.tableAlias=tableAlias;
        heapFile=(HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.heapFileIterator=(HeapFileIterator) (heapFile).iterator(this.transactionId);
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        this.heapFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     *         todo 这里返回含有别名的TupleDesc，我没有在这里将this.tableid对应的表的TupleDesc设置为含有别名的TupleDesc，从别名是依赖于某个查询语句而不是依赖表的角度考虑，这样是合理的。
     */
    public TupleDesc getTupleDesc() {
        TupleDesc tupleDesc=this.heapFile.getTupleDesc();
        Type[] typeAr=new Type[tupleDesc.numFields()];
        String[] fieldAr=new String[tupleDesc.numFields()];
        for (int i = 0; i < tupleDesc.numFields(); i++) {
            typeAr[i]=tupleDesc.getFieldType(i);
            fieldAr[i]=this.tableAlias+"."+tupleDesc.getFieldName(i);
        }
        return new TupleDesc(typeAr,fieldAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return this.heapFileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return this.heapFileIterator.next();
    }

    public void close() {
        this.heapFileIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this.heapFileIterator.rewind();
    }
}
