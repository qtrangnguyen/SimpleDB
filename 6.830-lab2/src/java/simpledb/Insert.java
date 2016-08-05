package simpledb;
import java.io.*;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends Operator {

    private TransactionId t;
    private DbIterator child;
    private int tableid;
    private boolean ifcalled = false;
    
    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        this.t = t;
        this.child = child;
        this.tableid = tableid;
        
        if(!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableid)))
            throw new DbException("TupleDesc of child differs from table into which we are to insert");
    }

    public TupleDesc getTupleDesc() {

        
        return child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
    }

    public void close() {
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext()
            throws TransactionAbortedException, DbException {

        if(ifcalled)
            return null;
        else
        {
            ifcalled = true;
            int count=0;
            try{
                while(child.hasNext())
                {
                    Tuple tup = child.next();
                    Database.getBufferPool().insertTuple(t,tableid,tup);    
                    count++;
                }
            }
            catch (IOException e)
            {
                e.printStackTrace();
            } 
            
            Type [] tarr = {Type.INT_TYPE};
            TupleDesc td = new TupleDesc(tarr);
            Tuple restup = new Tuple(td);
            restup.setField(0, new IntField(count));
            return restup;

        }
    }
}
