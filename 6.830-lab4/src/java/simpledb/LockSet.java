package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
/**
 * LockSet is used to implement locks needed by transactions
 */
public class LockSet {
    private final ConcurrentMap<PageId, Object> lockRegistry;
    private final Map<PageId, Set<TransactionId>> sharedLocks;
    private final Map<PageId, TransactionId> exclusiveLocks;
    private final Map<TransactionId, Set<PageId>> relatedPages;
    
    private final Map<TransactionId, PageId> wishLock;
    
    public LockSet()
    {
        lockRegistry = new ConcurrentHashMap<PageId, Object>();   
        sharedLocks = new HashMap<PageId, Set<TransactionId>>();   
        exclusiveLocks = new HashMap<PageId, TransactionId>();   
        relatedPages = new HashMap<TransactionId, Set<PageId>>(); 
        wishLock = new HashMap<TransactionId, PageId>();
        
    }

    public static LockSet create()
    {
    	return new LockSet();
    }
    
    private Object getLock(PageId pageId) {
    	lockRegistry.putIfAbsent(pageId, new Object());
		return lockRegistry.get(pageId);
	}
    
    private Set<TransactionId> getSharedLockList(PageId pageId) {
		if(sharedLocks.get(pageId)==null)
			sharedLocks.put(pageId,new HashSet<TransactionId>());
		return sharedLocks.get(pageId);
	}
    
    private Set<PageId> getReplatedPageList(TransactionId tid) {
    	if(relatedPages.get(tid)==null)
    		relatedPages.put(tid, new HashSet<PageId>());
		return relatedPages.get(tid);
	}
    
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException
    {
        if(perm==Permissions.READ_ONLY)
        {
            acquireSharedLock(tid,pid);
        }
        else if(perm==Permissions.READ_WRITE)
        {
            acquireExclusiveLock(tid,pid);
        }

    }

    public void releaseLock(TransactionId tid)
    {
    	Set<PageId> pageset = new HashSet<PageId>(getReplatedPageList(tid)); 
    	for(PageId pid: pageset)
    	{
    		releaseLock(tid,pid);
    	}
    }
    
    public void releaseLock(TransactionId tid, PageId pid)
    {
    	Object lock = getLock(pid);
    	while(true)
        {
            synchronized(lock)
            {
            	if(exclusiveLocks.get(pid)!=null)
            		if(exclusiveLocks.get(pid).equals(tid))
            			exclusiveLocks.put(pid,null);

          
            	Set<TransactionId> arrTran = getSharedLockList(pid);
            	arrTran.remove(tid);
            	
            	Set<PageId> arrPage = getReplatedPageList(tid);
            	arrPage.remove(pid);
            	return;
            }
        }
    }

    public void acquireSharedLock(TransactionId tid, PageId pid) throws TransactionAbortedException
    {
    	Object lock = getLock(pid);
    	wishLock.put(tid, pid);
        while(true)
        {
            synchronized(lock)
            {
            	deadlockTest(tid,pid,false);
            	
            	if(exclusiveLocks.get(pid)!=null)
            		if(!exclusiveLocks.get(pid).equals(tid))
            			continue;
            		else
            		{
            			wishLock.remove(tid);
            			return;
            		}
        			

            	//get shared lock
            	Set<TransactionId> arrTran = getSharedLockList(pid);
            	arrTran.add(tid);
            	//System.out.println(arrTran);

            	//record list
            	Set<PageId> arrPage = getReplatedPageList(tid);
            	arrPage.add(pid);
            	//System.out.println("end shared");
            	
            	wishLock.remove(tid);
            	return;
            }
        }
    }

    public void acquireExclusiveLock(TransactionId tid, PageId pid) throws TransactionAbortedException
    {
    	Object lock = getLock(pid);
    	wishLock.put(tid, pid);
    	while(true)
        {
            synchronized(lock)
            {
            	deadlockTest(tid,pid,true);
            	
            	if(exclusiveLocks.get(pid)!=null)
            		if(!exclusiveLocks.get(pid).equals(tid))
            			//ex-lock is acquired by others
            			continue;
            		else
            		{
            			//already have ex-lock
            			wishLock.remove(tid);
            			return;
            		}

            	if(getSharedLockList(pid).isEmpty())
            	{
            		//no one has shared lock, so get ex-lock
            		exclusiveLocks.put(pid, tid);

            		Set<PageId> arrPage = getReplatedPageList(tid);
            		arrPage.add(pid);
            		
            		wishLock.remove(tid);
            		return;
            	}
            	else if(getSharedLockList(pid).size()==1 & getSharedLockList(pid).iterator().next().equals(tid))
            	{
            		//update lock
            		getSharedLockList(pid).clear();
            		exclusiveLocks.put(pid, tid);
            		getReplatedPageList(tid).add(pid);
            		
            		wishLock.remove(tid);
            		return;

            	}    	
            }
        }
    }
    
    public boolean holdsLock(TransactionId tid, PageId pid)
    {
    	return getReplatedPageList(tid).contains(pid);
    }
    
    public void deadlockTest(TransactionId tid, PageId pid, boolean ifExclusive) throws TransactionAbortedException
    {

    	ArrayList<TransactionId> lockholders = new ArrayList<TransactionId>(getSharedLockList(pid));
    	if(exclusiveLocks.get(pid)!=null)
    		lockholders.add(exclusiveLocks.get(pid));

    	for(TransactionId holder: lockholders)
    	{
    		if (holder==null)
    			continue;
    		if (tid.equals(holder))
    			continue;
    		if (getReplatedPageList(tid).contains(wishLock.get(holder)))
    			throw new TransactionAbortedException();
    			
    	}

    	
   
    }
}
