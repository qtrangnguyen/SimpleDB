package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what; 
    private ArrayList<Tuple> TupleList; 
    private ArrayList<Integer> CountList; 
    private ArrayList<Integer> SumList; 
    private TupleDesc td;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        TupleList = new ArrayList<Tuple>();
        CountList = new ArrayList<Integer>();
        SumList = new ArrayList<Integer>();

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
                if(what==Op.MIN)
                {
                    t.setField(0,new IntField(Math.min(tValue,tupValue)));
                }
                else if(what==Op.MAX)
                {
                    t.setField(0,new IntField(Math.max(tValue,tupValue)));
                }
                else if(what==Op.SUM)
                {
                    t.setField(0,new IntField(tValue+tupValue));
                }
                else if(what==Op.AVG)
                {
                    CountList.set(0,CountList.get(0)+1);
                    SumList.set(0,SumList.get(0)+tupValue);
                    int n = SumList.get(0)/CountList.get(0);
                    t.setField(0,new IntField(n));
                }
                else if(what==Op.COUNT)
                {
                    t.setField(0,new IntField(tValue+1));
                }
            }
            else
            {
                if(what==Op.AVG)
                {
                    CountList.add(1);
                    SumList.add(tupValue);
                }
                else if(what==Op.COUNT)
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
                    int tupValue = ((IntField)tup.getField(afield)).getValue();
                    int tValue = ((IntField)t.getField(1)).getValue();
                    if(what==Op.MIN)
                    {
                        t.setField(1,new IntField(Math.min(tValue,tupValue)));
                    }
                    else if(what==Op.MAX)
                    {
                        t.setField(1,new IntField(Math.max(tValue,tupValue)));
                    }
                    else if(what==Op.SUM)
                    {
                        t.setField(1,new IntField(tValue+tupValue));
                    }
                    else if(what==Op.AVG)
                    {
                        CountList.set(i,CountList.get(i)+1);
                        SumList.set(i,SumList.get(i)+tupValue);
                        t.setField(1,new IntField(SumList.get(i)/CountList.get(i)));
                    }
                    else if(what==Op.COUNT)
                    {
                        t.setField(1,new IntField(tValue+1));
                    }
                    return;
                }
            }

            Tuple t = new Tuple(td);
            Field f0 = tup.getField(gbfield);
            Field f1 = tup.getField(afield);
            

            if(what==Op.AVG)
            {
                CountList.add(1);
                SumList.add(((IntField)tup.getField(afield)).getValue());
            }
            else if(what==Op.COUNT)
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
