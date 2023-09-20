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
        this.buckets=10;// todo 暂时写死为10个桶 1.注意最后一个桶的范围不一定会到下面计算出的bucketRight，要特判 2.注意负数情况，计算key以min为base；min到max之间的数字越多，由于桶的数量固定，所以计算等于一个calue得到的可能性会越来越不准确，增加桶可以增加准确性
        this.min=min;
        this.max=max;
        this.realBuckets=new HashMap<>();
        int width=max-(min-1);
        int bucketWidth=width/this.buckets;
        if(width%this.buckets!=0)
            bucketWidth++;//有余数，此时桶的范围不能覆盖min到max的范围，每个桶里面多放一个，肯定够
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
        int key=(v-min)/this.bucketWidth;
        if(!this.realBuckets.containsKey(key)){
            //do nothing
        }else {
            count=this.realBuckets.get(key);
        }
        // 下列范围，左开右闭，对于lab3文档的举例就是(0, 10]的样子
        int bucketRight=min-1+((v-min)/this.bucketWidth+1)*this.bucketWidth;
        int bucketLeft=min-1+((v-min)/this.bucketWidth)*this.bucketWidth;
        // 用于下列计算的bucketWidth
        int bucketWidth=this.bucketWidth;
        if(key==buckets-1){//最后一个桶的bucketWidth可能比前面的短：桶的长度笃定、个数固定，可能刚好覆盖(max-(min-1))的范围，也可能比这个范围大
            bucketWidth-=(bucketRight-max);
        }
        double percent;
        switch (op){//    3/210*21+2/210*21+2/210*21+2/210*21+1/210*16  63+42+42+42+16  138+63  201
            case EQUALS:
                return  count *1.0 /bucketWidth/ntups;
            case NOT_EQUALS:
                return  1.0-count*1.0 /bucketWidth/ntups;
            case GREATER_THAN:
                selectivity=count*1.0/ntups;
                percent= (bucketRight-v)*1.0/bucketWidth;
                selectivity*=percent;
                for(Map.Entry<Integer, Integer> entry: realBuckets.entrySet()){
                    if(entry.getKey()>key){
                        selectivity+=entry.getValue()*1.0/ntups;
                    }
                }
                return selectivity;
            case GREATER_THAN_OR_EQ:
                selectivity=count*1.0/ntups;
                percent= (bucketRight-v+1)*1.0/bucketWidth;
                selectivity*=percent;
                for(Map.Entry<Integer, Integer> entry: realBuckets.entrySet()){
                    if(entry.getKey()>key){
                        selectivity+=entry.getValue()*1.0/ntups;
                    }
                }
                return selectivity;
            case LESS_THAN:
                selectivity=count*1.0/ntups;
                percent= (v-bucketLeft-1)*1.0/bucketWidth;
                selectivity*=percent;
                for(Map.Entry<Integer, Integer> entry: realBuckets.entrySet()){
                    if(entry.getKey()<key){
                        selectivity+=entry.getValue()*1.0/ntups;
                    }
                }
                return selectivity;
            case LESS_THAN_OR_EQ:
                selectivity=count*1.0/ntups;
                percent= (v-bucketLeft)*1.0/bucketWidth;
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
