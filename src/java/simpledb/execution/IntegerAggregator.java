package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;

    private List<Tuple> tupleList=new ArrayList<>();

    private TupleDesc tupleDesc;

    private Map<Field,Integer> valMap=new HashMap<>();

    private Map<Field,Integer> countMap=new HashMap<>();

    private OpIterator opIterator;


    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
       this.gbfield=gbfield;
       this.gbfieldtype=gbfieldtype;
       this.afield=afield;
       this.op=what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
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
                typeAr[1]=tup.getTupleDesc().getFieldType(afield);
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
        IntField intField=(IntField) tup.getField(afield);
        int aggregateVal=intField.getValue();
        Integer f;
        if(valMap.containsKey(groupVal)){
            f=valMap.get(groupVal);
            int updatedCount=countMap.get(groupVal)+1;
            countMap.put(groupVal,updatedCount);
        }else {
            valMap.put(groupVal,aggregateVal);
            countMap.put(groupVal,1);
            return;
        }
        switch (op){
            case MIN:
                if(aggregateVal<f)
                    valMap.replace(groupVal, aggregateVal);
                break;
            case MAX:
                if(aggregateVal>f)
                    valMap.replace(groupVal, aggregateVal);
                break;
            case AVG:
            case SUM:
                int sum=f+aggregateVal;
                valMap.replace(groupVal,sum);
                break;
            case COUNT:
                ;//上面已经计数过
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        if(gbfield==-1){
            switch (op){
                case MIN:
                case MAX:
                case SUM:
                    Tuple tupleSingleVal=new Tuple(tupleDesc);
                    int min = valMap.get(new IntField(0));
                    tupleSingleVal.setField(0, new IntField(min));
                    tupleList.add(tupleSingleVal);
                    break;//break;不能忘。。
                case AVG:
                    Tuple tupleAvg=new Tuple(tupleDesc);
                    int sum = valMap.get(new IntField(0));
                    int count =countMap.get(new IntField(0));
                    tupleAvg.setField(0, new IntField(sum/count));
                    tupleList.add(tupleAvg);
                    break;
                case COUNT:
                    Tuple tupleCount=new Tuple(tupleDesc);
                    tupleCount.setField(0, new IntField(countMap.get(new IntField(0))));
                    tupleList.add(tupleCount);
            }
            return new TupleIterator(tupleDesc, tupleList);
        }

        switch (op){
            case MIN:
            case MAX:
            case SUM:
                for (Map.Entry<Field, Integer> entry : valMap.entrySet()) {
                    Tuple tupleSingleVal=new Tuple(tupleDesc);
                    tupleSingleVal.setField(0,entry.getKey());
                    tupleSingleVal.setField(1,new IntField(entry.getValue()));
                    tupleList.add(tupleSingleVal);
                }
                break;
            case AVG:
                for (Map.Entry<Field, Integer> entry : valMap.entrySet()) {
                    Tuple tupleAvg=new Tuple(tupleDesc);
                    tupleAvg.setField(0,entry.getKey());
                    int count = countMap.get(entry.getKey());
                    tupleAvg.setField(1,new IntField(entry.getValue()/count));
                    tupleList.add(tupleAvg);
                }
                break;
            case COUNT:
                for (Map.Entry<Field, Integer> entry : valMap.entrySet()) {
                    Tuple tupleCount=new Tuple(tupleDesc);
                    tupleCount.setField(0,entry.getKey());
                    int count = countMap.get(entry.getKey());
                    tupleCount.setField(1,new IntField(count));
                    tupleList.add(tupleCount);
                }
        }
        return new TupleIterator(tupleDesc,tupleList);
    }

}
