package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;

    private Aggregator aggregator;

    private OpIterator opIterator;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child=child;
        this.afield=afield;
        this.gfield=gfield;
        this.aop=aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        if(this.gfield==-1)
            return Aggregator.NO_GROUPING;
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        if(this.gfield==-1)
            return null;
        return this.child.getTupleDesc().getFieldName(this.gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return this.child.getTupleDesc().getFieldName(this.afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        child.open();
        super.open();
        Type afieldType=child.getTupleDesc().getFieldType(afield);
        Type gfieldType=child.getTupleDesc().getFieldType(gfield);
        if(afieldType== Type.INT_TYPE){
            aggregator=new IntegerAggregator(gfield,gfieldType,afield,aop);
        }else  if(afieldType== Type.STRING_TYPE){
            aggregator=new StringAggregator(gfield,gfieldType,afield,aop);
        }
        while(child.hasNext()){
            Tuple tuple=child.next();
            aggregator.mergeTupleIntoGroup(tuple);
        }
        opIterator=aggregator.iterator();
        opIterator.open();//这里的opIterator是aggregator.iterator()返回的TupleIterator，也需要open初始化内部的Iterator
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(opIterator.hasNext())
            return opIterator.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        opIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        if(gfield==-1){
            Type[] typeAr = new Type[1];
            String[] fieldAr = new String[1];
            typeAr[0]=Type.INT_TYPE;
            fieldAr[0]=child.getTupleDesc().getFieldName(afield)+"("+aop+")";
            return new TupleDesc(typeAr,fieldAr);
        }
        Type[] typeAr = new Type[2];
        String[] fieldAr = new String[2];
        typeAr[0]=child.getTupleDesc().getFieldType(gfield);
        fieldAr[0]="groupVal";
        typeAr[1]=Type.INT_TYPE;
        fieldAr[1]=child.getTupleDesc().getFieldName(afield)+"("+aop+")";
        return new TupleDesc(typeAr,fieldAr);
    }

    public void close() {
        opIterator.close();
        super.close();
        child.close();
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
