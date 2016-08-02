package simpledb;
import java.util.*;

/**
 * DbFileIterator is the iterator interface that all SimpleDB Dbfile should
 * implement.
 */
public class HeapFileIterator implements DbFileIterator {
    private Iterator<Tuple> iterator=null;
    private int pageIndex = 0;
    private TransactionId tid;
    private HeapFile file;
    private boolean ifopen = false;

    public HeapFileIterator(TransactionId tid, HeapFile f) {
        this.tid = tid;
        this.file = f;
    }
    
    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    @Override
    public void open()
        throws DbException, TransactionAbortedException{
        pageIndex = 0;
        PageId pageId = new HeapPageId(file.getId(), pageIndex);
        Page page = Database.getBufferPool().getPage(tid,pageId,Permissions.READ_ONLY);
        HeapPage heappage = (HeapPage)page;
        iterator = heappage.iterator();

        ifopen = true;
    }

    /** @return true if there are more tuples available. */
    @Override
    public boolean hasNext()
        throws DbException, TransactionAbortedException{
        if(!ifopen)
            open();
        if(iterator==null)
            return false;

        if(iterator.hasNext())
            return true;
        else{
            if(pageIndex>=file.numPages()-1)
                return false;
            else{
                PageId pageId = new HeapPageId(file.getId(), pageIndex+1);
                Page page = Database.getBufferPool().getPage(tid,pageId,Permissions.READ_ONLY);
                HeapPage heappage = (HeapPage)page;
                return heappage.iterator().hasNext();
            }
        }
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    @Override
    public Tuple next()
        throws DbException, TransactionAbortedException, NoSuchElementException{
        if(!ifopen)
            open();
        if(iterator==null)
            throw new NoSuchElementException();
        
        if (iterator.hasNext())
            return iterator.next();
        else
        {
            PageId pageId = new HeapPageId(file.getId(), pageIndex+1);
            Page page = Database.getBufferPool().getPage(tid,pageId,Permissions.READ_ONLY);
            HeapPage heappage = (HeapPage)page;
            
            if (page!=null)
                if (heappage.iterator().hasNext())
                {
                    pageIndex++;
                    iterator = heappage.iterator();
                    return iterator.next();
                }
            throw new NoSuchElementException();

        }

        
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    @Override
    public void rewind() throws DbException, TransactionAbortedException{
        close();
        open();

    }

    /**
     * Closes the iterator.
     */
    @Override
    public void close(){
        iterator = null;
        pageIndex = 0;
    }
}
