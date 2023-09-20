package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;//buckets的数量

    private int min;
    private int max;

    private int bucketWidth;
    private int ntups;

    private Map<Integer, Integer> realBuckets;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.；要求时间和空间复杂度不能是线性的
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     * ；因为是整数，所以可以i进行求一下除数，然后使用hashmap，这样除了对于tuple的循环遍历，内部的复杂度为1，也就是内部不会再有排序的开销
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets=5;
        this.min=min;
        this.max=max;
        this.realBuckets=new HashMap<>();
        int width=max-min;
        int bucketWidth=width/this.buckets;
        this.bucketWidth=bucketWidth;
        this.ntups=0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int key=(v-min)/this.bucketWidth;
        if(this.realBuckets.containsKey(key)){
            Integer height=this.realBuckets.get(key);
            height++;
            this.realBuckets.put(key,height);
            ntups++;
            return;
        }
        Integer height=1;
        this.realBuckets.put(key,height);
        ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int count=0;
        double selectivity;
        int key=(v-min)/bucketWidth;
        if(!this.realBuckets.containsKey(key)){
            //do nothing
        }else {
            count=this.realBuckets.get(key);
        }
        int bucketRight=min+((v-min)/bucketWidth+1)*bucketWidth;
        int bucketLeft=min+((v-min)/bucketWidth)*bucketWidth;
        double percent;
        switch (op){
            case EQUALS:
                return  count *1.0 /bucketWidth/ntups;
            case NOT_EQUALS:
                return  1.0-count*1.0 /bucketWidth/ntups;
            case GREATER_THAN:
                selectivity=count*1.0/ntups;
                percent= (bucketRight-v-1)*1.0/bucketWidth;
                selectivity*=percent;
                for(Map.Entry<Integer, Integer> entry: realBuckets.entrySet()){
                    if(entry.getKey()>key){
                        selectivity+=entry.getValue()*1.0/ntups;
                    }
                }
                return selectivity;
            case GREATER_THAN_OR_EQ:
                selectivity=count*1.0/ntups;
                percent= (bucketRight-v)*1.0/bucketWidth;
                selectivity*=percent;
                for(Map.Entry<Integer, Integer> entry: realBuckets.entrySet()){
                    if(entry.getKey()>key){
                        selectivity+=entry.getValue()*1.0/ntups;
                    }
                }
                return selectivity;
            case LESS_THAN:
                selectivity=count*1.0/ntups;
                percent= (v-bucketLeft)*1.0/bucketWidth;
                selectivity*=percent;
                for(Map.Entry<Integer, Integer> entry: realBuckets.entrySet()){
                    if(entry.getKey()<key){
                        selectivity+=entry.getValue()*1.0/ntups;
                    }
                }
                return selectivity;
            case LESS_THAN_OR_EQ:
                selectivity=count*1.0/ntups;
                percent= (v-bucketLeft+1)*1.0/bucketWidth;
                selectivity*=percent;
                for(Map.Entry<Integer, Integer> entry: realBuckets.entrySet()){
                    if(entry.getKey()<key){
                        selectivity+=entry.getValue()*1.0/ntups;
                    }
                }
                return selectivity;
        }
    	// some code goes here
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
