package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	private int bucketsNum;
	private int min;
	private int max;
	private int totalNum;
	private double width;
	private int buckets[];
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
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.bucketsNum = buckets;
    	this.min = min;
    	this.max = max;
    	this.buckets = new int [buckets];
    	for(int i=0;i<this.bucketsNum;++i)
    		this.buckets[i] = 0;
    	
    	this.width = ((double)(this.max-this.min))/this.bucketsNum;
    	this.totalNum = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	
    	this.buckets[getBucketIndex(v)] += 1;
    	this.totalNum += 1;
    }

    private int getBucketIndex(int v)
    {
    	if(v>=this.max)
    		return this.bucketsNum-1;
    	else if(v<this.min)
    		return 0;
    	
    	double bucketIndexD = ((double)(v-this.min))/this.width;
    	int bucketIndex = (int)bucketIndexD;
    	return bucketIndex;
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
    	if(op.equals(Predicate.Op.EQUALS))
    	{
    		if(v>this.max|v<this.min)
    			return 0.0;
    		return (double)buckets[getBucketIndex(v)]/width/totalNum;
    	}
    	else if(op.equals(Predicate.Op.NOT_EQUALS))
    	{
    		if(v>this.max|v<this.min)
    			return 1.0;
    		return 1-(double)buckets[getBucketIndex(v)]/width/totalNum;
    	}
    	else if(op.equals(Predicate.Op.GREATER_THAN) | op.equals(Predicate.Op.GREATER_THAN_OR_EQ))
    	{
    		if(v>this.max)
    			return 0.0;
    		else if(v<=this.min)
    			return 1.0; 
    		
    		double selectivity = 0;
    		
    		double b_part = b_partCalc(v,op);
    		double b_f = (double)buckets[getBucketIndex(v)]/totalNum;
    		selectivity += b_part * b_f;
    		
    		int count = 0;
    		for(int i=getBucketIndex(v)+1;i<bucketsNum;++i)
    		{
    			count += buckets[i];
    		}
    		selectivity += (double)count/totalNum;
    		return selectivity;
    	}
    	else if(op.equals(Predicate.Op.LESS_THAN)| op.equals(Predicate.Op.LESS_THAN_OR_EQ) )
    	{
    		if(v<this.min)
    			return 0.0;
    		else if(v>=this.max)
    			return 1.0; 
    		
    		double selectivity = 0;
    		
    		double b_part = b_partCalc(v,op);
    		
    		double b_f = (double)buckets[getBucketIndex(v)]/totalNum;
    		selectivity += b_part * b_f;
    		
    		int count = 0;
    		for(int i=getBucketIndex(v)-1;i>=0;--i)
    		{
    			count += buckets[i];
    		}
    		selectivity += (double)count/totalNum;
    		return selectivity;
    	}
    	// some code goes here
        return -1.0;

    }
    
    private double b_partCalc(int v, Predicate.Op op)
    {
    	double lower = min + getBucketIndex(v)*width;
		double upper = lower + width;
		
		double totalInt = Math.floor(upper)-Math.ceil(lower)+1;
		double coveredInt = 0;
		
    	if(op.equals(Predicate.Op.LESS_THAN_OR_EQ))
    		coveredInt = v-Math.ceil(lower)+1;
    	else if(op.equals(Predicate.Op.LESS_THAN))
    		coveredInt = v-Math.ceil(lower);
    	else if(op.equals(Predicate.Op.GREATER_THAN))
    		coveredInt = Math.floor(upper)-v;
    	else if(op.equals(Predicate.Op.GREATER_THAN_OR_EQ))
    		coveredInt = Math.floor(upper)-v+1;
    	
    	return coveredInt/totalInt;
    }
   
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        String desc = "bucketsNum: "+bucketsNum+", min: "+min+", max: "+max;
        return desc;
    }
}
