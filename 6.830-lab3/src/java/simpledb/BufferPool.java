package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    public ArrayList<Page> PageList;
    public int MaxSize = DEFAULT_PAGES;

    private final LockSet lockset;
    
    private final Map<TransactionId,Set<PageId>> AffectedPageSetByTran;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        PageList = new ArrayList<Page>();
        MaxSize = numPages; 

        lockset = LockSet.create();
        AffectedPageSetByTran = new HashMap<TransactionId,Set<PageId>>();
    }
    
    private Set<PageId> getAffectedPageSet(TransactionId tid) {
        if(AffectedPageSetByTran.get(tid)==null)
        	AffectedPageSetByTran.put(tid, new HashSet<PageId>());
    	return AffectedPageSetByTran.get(tid);
      }
    
    public static int getPageSize() {
      return PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

    		lockset.acquireLock(tid, pid, perm);
    		
            getAffectedPageSet(tid).add(pid);
            
            for(Page page: PageList)
            {
                if(page.getId().equals(pid))
                {
                    return page;
                }
            }
                
            if(PageList.size()==MaxSize)
            //    throw new DbException("PageList is full!");
                evictPage();

            for(Table tab: Database.getCatalog().getTables())
            {
                if(tab.file.getId()==pid.getTableId())
                {
                    Page page = tab.file.readPage(pid);
                    PageList.add(page);
                    return page;
                }
            }

            return null;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
    	lockset.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
    	
    	transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
    	return lockset.holdsLock(tid,p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
    	if(commit)
    		for(PageId pid: getAffectedPageSet(tid))
    			flushPage(pid);
    	else
    	{
    		for(PageId pid: getAffectedPageSet(tid))
    		{
    			readPage(pid);
    		}
    		
    	}
    	getAffectedPageSet(tid).clear();
    	lockset.releaseLock(tid);
    	
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	
    	List<Page> dirtiedPages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid,t);
        for(Page p: dirtiedPages)
        {
        	p.markDirty(true,tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
    	PageId pid = t.getRecordId().getPageId();
    	Page dirtiedPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).deleteTuple(tid,t);
    	dirtiedPage.markDirty(true,tid);
        
        
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for(Page page: PageList)
        {
        	if(page.isDirty()!=null) continue;
        	
            DbFile file = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
            file.writePage(page);
        }
        PageList.clear();
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        for(Page page: PageList)
        {
            if(page.getId().equals(pid))
            {
                DbFile file = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
                file.writePage(page);
                PageList.remove(page);
                return;
            }
        }

    }
    
    private synchronized void readPage(PageId pid) throws IOException {
        for(Page page: PageList)
        {
            if(page.getId().equals(pid))
            {
            	for(Table tab: Database.getCatalog().getTables())
                {
                    if(tab.file.getId()==pid.getTableId())
                    {
                        Page diskpage = tab.file.readPage(pid);
                        PageList.remove(page);
                        PageList.add(diskpage);
                        return;
                    }
                }
            }
        }

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
    	for(PageId pid:getAffectedPageSet(tid))
    	{
    		flushPage(pid);
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
    	for(Page discardPage: PageList)
    	{
    		if(discardPage.isDirty()!=null) continue;
    		
    		try{
                flushPage(discardPage.getId());
            }
            catch(IOException e)
            {
                throw new DbException("error occurs when evictPage");
            }
    		return;
    	}
    	throw new DbException("no clean page to evict");
        
    }

}
