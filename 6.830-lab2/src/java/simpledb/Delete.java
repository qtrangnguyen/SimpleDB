package simpledb;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends Operator {

    private TransactionId t;
    private DbIterator child;
    private boolean ifcalled = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.t = t;
        this.child = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (ifcalled)
           return null;
        else
           ifcalled = true;
        
        int count=0;
        while(child.hasNext())
        {
            Tuple tup = child.next();
            Database.getBufferPool().deleteTuple(t,tup);    
            count++;
        }
        
        Type [] tarr = {Type.INT_TYPE};
        TupleDesc td = new TupleDesc(tarr);
        Tuple restup = new Tuple(td);
        restup.setField(0, new IntField(count));
        return restup;

    }
}
