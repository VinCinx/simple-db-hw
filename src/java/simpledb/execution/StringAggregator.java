package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;

    private List<Tuple> tupleList=new ArrayList<>();

    private TupleDesc tupleDesc;

    private Map<Field,Integer> countMap=new HashMap<>();

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        if(what!=Op.COUNT)
            throw new IllegalArgumentException();
        this.op=what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(tupleDesc==null){
            Type[] typeAr;
            String[] fieldAr;
            if(gbfield==-1){
                typeAr=new Type[1];
                fieldAr=new String[1];
                typeAr[0]=tup.getTupleDesc().getFieldType(afield);
                fieldAr[0]=tup.getTupleDesc().getFieldName(afield)+"("+op+")";
                tupleDesc=new TupleDesc(typeAr,fieldAr);
            }else {
                typeAr=new Type[2];
                fieldAr=new String[2];
                typeAr[0]=gbfieldtype;
                typeAr[1]=Type.INT_TYPE;//String类型只需要实现count，typeAr[1]只要是int，计数就可以了
                fieldAr[0]="groupVal";
                fieldAr[1]=tup.getTupleDesc().getFieldName(afield)+"("+op+")";
                tupleDesc=new TupleDesc(typeAr,fieldAr);
            }
        }

        Field groupVal;
        if(gbfield==-1){
            groupVal=new IntField(0);
        }else {
            groupVal=tup.getField(gbfield);
        }
        if(countMap.containsKey(groupVal)){
            int updatedCount=countMap.get(groupVal)+1;
            countMap.put(groupVal,updatedCount);
        }else {
            countMap.put(groupVal,1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        if(gbfield==-1){
            Tuple tupleCount=new Tuple(tupleDesc);
            tupleCount.setField(0, new IntField(countMap.get(new IntField(0))));
            tupleList.add(tupleCount);
            return new TupleIterator(tupleDesc,tupleList);
        }
        countMap.entrySet().forEach(entry -> {
            Tuple tupleCount = new Tuple(tupleDesc);
            tupleCount.setField(0, entry.getKey());
            int count = countMap.get(entry.getKey());
            tupleCount.setField(1, new IntField(count));
            tupleList.add(tupleCount);
        });
        return new TupleIterator(tupleDesc,tupleList);
    }

}
