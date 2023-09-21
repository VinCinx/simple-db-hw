package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    private int tableId;
    private int ioCostPerPage;

    private int totalTuple;
    private Map<Integer, IntHistogram> intHistogramMap=new HashMap<>();
    private Map<Integer, StringHistogram> stringHistogramMap=new HashMap<>();

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        this.tableId=tableid;
        this.ioCostPerPage=ioCostPerPage;
        DbFile file = Database.getCatalog().getDatabaseFile(tableid);
        SeqScan seqScan=new SeqScan(new TransactionId(),tableid,"");
        //为了减少扫描的次数，不直接使用lab2已经实现的Aggregate计算最大最小值，而且lab2无法计算String的最大最小值
        try {
            Map<Integer,Integer> maxValMap=new HashMap<>();
            Map<Integer,Integer> minValMap=new HashMap<>();
            seqScan.open();
            while(seqScan.hasNext()){
                Tuple tuple=seqScan.next();
                this.totalTuple++;
                for (int i = 0; i < tuple.getTupleDesc().numFields(); i++) {
                    Type type=tuple.getTupleDesc().getFieldType(i);
                    if(type==Type.INT_TYPE){
                        IntField field=(IntField) tuple.getField(i);
                        int val=field.getValue();
                        if(maxValMap.containsKey(i)){
                            int maxVal=maxValMap.get(i);
                            if(val>maxVal) maxVal=val;
                            maxValMap.put(i,maxVal);
                        }else {
                            maxValMap.put(i,val);
                        }
                        if(minValMap.containsKey(i)){
                            int minVal=minValMap.get(i);
                            if(val<minVal) minVal=val;
                            minValMap.put(i,minVal);
                        }else{
                            minValMap.put(i,val);
                        }
                    }else if(type==Type.STRING_TYPE){
                        StringField field=(StringField) tuple.getField(i);
                        String s=field.getValue();
                        if(stringHistogramMap.containsKey(i)){
                            StringHistogram stringHistogram=stringHistogramMap.get(i);
                            stringHistogram.addValue(s);
                        }else {
                            StringHistogram stringHistogram=new StringHistogram(100);
                            stringHistogram.addValue(s);
                            stringHistogramMap.put(i,stringHistogram);
                        }
                    }
                }
            }
            for (Map.Entry<Integer, Integer> entry : maxValMap.entrySet()) {
                int key=entry.getKey();
                int maxVal=entry.getValue();
                int minVal=minValMap.get(key);
                IntHistogram intHistogram=new IntHistogram(100,minVal,maxVal);
                intHistogramMap.put(key,intHistogram);
            }
            // 重置
            seqScan.rewind();
            while(seqScan.hasNext()) {
                Tuple tuple = seqScan.next();
                for (int i = 0; i < tuple.getTupleDesc().numFields(); i++) {
                    Type type = tuple.getTupleDesc().getFieldType(i);
                    if (type == Type.INT_TYPE) {
                        IntField field=(IntField) tuple.getField(i);
                        int val=field.getValue();
                        intHistogramMap.get(i).addValue(val);
                    }
                }
            }
            seqScan.close();
        } catch (TransactionAbortedException | DbException e) {
            throw new RuntimeException(e);
        }

        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        HeapFile heapFile=new HeapFile(((HeapFile)Database.getCatalog().getDatabaseFile(tableId)).getFile(), Database.getCatalog().getTupleDesc(tableId));
        return heapFile.numPages()*ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (totalTuple*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if(intHistogramMap.containsKey(field)){
            IntHistogram intHistogram=intHistogramMap.get(field);
            return intHistogram.estimateSelectivity(op,((IntField)constant).getValue());
        }
        StringHistogram stringHistogram=stringHistogramMap.get(field);
        return stringHistogram.estimateSelectivity(op,((StringField)constant).getValue());
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return this.totalTuple;
    }

}
