package simpledb;
import java.util.*;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what; 
    private ArrayList<Tuple> TupleList; 
    private TupleDesc td;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        TupleList = new ArrayList<Tuple>();

        //set tupledesc
        if(gbfield==Aggregator.NO_GROUPING)
        {
            Type[] typeAr = {Type.INT_TYPE};
            String[] fieldAr = {""};
            td = new TupleDesc(typeAr,fieldAr);
        }
        else
        {
            Type[] typeAr = {
                gbfieldtype,
                Type.INT_TYPE};
            String[] fieldAr = {
                "",""};
            td = new TupleDesc(typeAr,fieldAr);
        }

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(gbfield==Aggregator.NO_GROUPING)
        {
            int tupValue = ((IntField)tup.getField(afield)).getValue();
            if(TupleList.size()!=0)
            {
                Tuple t = TupleList.get(0);
                int tValue = ((IntField)t.getField(0)).getValue();
                if(what==Op.COUNT)
                {
                    t.setField(0,new IntField(tValue+1));
                }
            }
            else
            {
                if(what==Op.COUNT)
                {
                    tupValue = 1;
                }

                Tuple t = new Tuple(td);
                t.setField(0,new IntField(tupValue));
                TupleList.add(t);
                
            }
        }
        else
        {
            for(int i=0;i<TupleList.size();++i)
            {
                Tuple t = TupleList.get(i);
                if(t.getField(0).equals(tup.getField(gbfield)))
                {
                    int tValue = ((IntField)t.getField(1)).getValue();
                    if(what==Op.COUNT)
                    {
                        t.setField(1,new IntField(tValue+1));
                    }
                    return;
                }
            }

            Tuple t = new Tuple(td);
            Field f0 = tup.getField(gbfield);
            Field f1 = tup.getField(afield);

            if(what==Op.COUNT)
            {
                f1 = new IntField(1);
            }
            t.setField(0,f0);
            t.setField(1,f1);
            TupleList.add(t);

        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        Iterable<Tuple> it = TupleList;
        return new TupleIterator(td,it);
    }

}
