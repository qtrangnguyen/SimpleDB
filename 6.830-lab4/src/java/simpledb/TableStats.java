package simpledb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

/** TableStats represents statistics (e.g., histograms) about base tables in a query */
public class TableStats {
    
	private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();
	
    /**
     * Number of bins for the histogram.
     * Feel free to increase this value over 100,
     * though our tests assume that you have at least 100 bins in your histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private DbFile file;
    private int ioCostPerPage;
    private int numTuples;
    private int numPages;

    /**
     * Create a new TableStats object, that keeps track of statistics on each column of a table
     * 
     * @param tableid The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO.  
     * 		                This doesn't differentiate between sequential-scan IO and disk seeks.
     */
    public TableStats (int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the DbFile for the table in question,
    	// then scan through its tuples and calculate the values that you need.
    	// You should try to do this reasonably efficiently, but you don't necessarily
    	// have to (for example) do everything in a single scan of the table.
    	// some code goes here
    	
    	this.file = Database.getCatalog().getDatabaseFile(tableid);
    	this.ioCostPerPage = ioCostPerPage;

    }
    
    public static void setTableStats(String tablename, TableStats stats)
    {
    	statsMap.put(tablename, stats);
    }
    
    public static ConcurrentHashMap<String, TableStats> getStatsMap()
    {
    	return statsMap;
    }

    /** 
     * Estimates the
     * cost of sequentially scanning the file, given that the cost to read
     * a page is costPerPageIO.  You can assume that there are no
     * seeks and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once,
     * so if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page.  (Most real hard drives can't efficiently
     * address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */ 
    public double estimateScanCost() {
    	// some code goes here
    	this.numPages = ((HeapFile)file).numPages();
        return this.numPages*ioCostPerPage;
    }

    /** 
     * This method returns the number of tuples in the relation,
     * given that a predicate with selectivity selectivityFactor is
     * applied.
	 *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified selectivityFactor
     * @throws TransactionAbortedException 
     * @throws DbException 
     */
    public int estimateTableCardinality(double selectivityFactor) {
    	// some code goes here
    	
    	HeapFileIterator it = (HeapFileIterator)this.file.iterator(new TransactionId());
		int count = 0;
        try {
			it.open();

	        while(it.hasNext())
	        {
	        	it.next();
	        	count++;
	        }

		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    	return (int) (count*selectivityFactor);
    }

    /** 
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the table.
     * 
     * @param field The field over which the predicate ranges
     * @param op The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {

    	
    	if(this.file.getTupleDesc().getFieldType(field).equals(Type.INT_TYPE))
    	{

        	HeapFileIterator it = (HeapFileIterator)this.file.iterator(new TransactionId());
        	ArrayList<Integer> arr = new ArrayList<Integer>();
        	
            try {
    			it.open();

    	        while(it.hasNext())
    	        {
    	        	arr.add(((IntField)it.next().getField(field)).getValue());
    	        	
    	        }

    		} catch (DbException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		} catch (TransactionAbortedException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
            
            int minValue = Collections.min(arr);
            int maxValue = Collections.max(arr);
        	IntHistogram ih = new IntHistogram(maxValue-minValue>NUM_HIST_BINS?NUM_HIST_BINS:maxValue-minValue,minValue,maxValue);
        	for(int v:arr)
        		ih.addValue(v);
        	return ih.estimateSelectivity(op, ((IntField)constant).getValue());
    	}

    	return 1.0;
    }

}
