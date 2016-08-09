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
    private final Map<PageId, ArrayList<TransactionId>> sharedLocks;
    private final Map<PageId, TransactionId> exclusiveLocks;
    private final Map<TransactionId, Set<PageId>> relatedPages;

    public LockSet()
    {
        lockRegistry = new ConcurrentHashMap<PageId, Object>();   
        sharedLocks = new HashMap<PageId, ArrayList<TransactionId>>();   
        exclusiveLocks = new HashMap<PageId, TransactionId>();   
        relatedPages = new HashMap<TransactionId, Set<PageId>>(); 
        
    }

    public static LockSet create()
    {
    	return new LockSet();
    }
    
    private Object getLock(PageId pageId) {
    	lockRegistry.putIfAbsent(pageId, new Object());
		return lockRegistry.get(pageId);
	}
    
    private ArrayList<TransactionId> getSharedLockList(PageId pageId) {
    	ArrayList<TransactionId> arrTran = sharedLocks.get(pageId);
		if(arrTran==null)
			arrTran = new ArrayList<TransactionId>();
		return arrTran;
	}
    
    private Set<PageId> getLockSet(TransactionId tid) {
    	Set<PageId> arrPage = relatedPages.get(tid);
    	if(arrPage==null)
			arrPage = new HashSet<PageId>();
		return arrPage;
	}
    
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm)
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
    	for(PageId pid: getLockSet(tid))
    	{
    		releaseLock(tid,pid);
    	}
    }
    
    public void releaseLock(TransactionId tid, PageId pid)
    {
    	while(true)
        {
        	
            synchronized(getLock(pid))
            {
            	if(exclusiveLocks.get(pid)==tid)
            		exclusiveLocks.put(pid,null);
            	
          
            	ArrayList<TransactionId> arrTran = getSharedLockList(pid);
            	arrTran.remove(tid);
            	
            	Set<PageId> arrPage = getLockSet(tid);
            	arrPage.remove(pid);
            	return;
            }
        }
    }

    public void acquireSharedLock(TransactionId tid, PageId pid)
    {
    	//System.out.println("begin shared");
        while(true)
        {
        	
            synchronized(getLock(pid))
            {
            	if(exclusiveLocks.get(pid)==null | exclusiveLocks.get(pid)==tid)
            	{
            		ArrayList<TransactionId> arrTran = getSharedLockList(pid);
            		arrTran.add(tid);
            		//System.out.println(arrTran);
            	
            		Set<PageId> arrPage = getLockSet(tid);
            		arrPage.add(pid);
            		//System.out.println("end shared");
            		return;
            	}
            }
        }
    }

    public void acquireExclusiveLock(TransactionId tid, PageId pid)
    {
    	//System.out.println("begin exclusive");
    	while(true)
        {
        	
            synchronized(getLock(pid))
            {
            	if(exclusiveLocks.get(pid)==null | exclusiveLocks.get(pid)==tid)
            	{
            		if(getSharedLockList(pid).isEmpty())
            		{
            			//System.out.println(getSharedLockList(pid));
            			//System.out.println("get lock!");
            			exclusiveLocks.put(pid, tid);
            	
            			Set<PageId> arrPage = getLockSet(tid);
            			arrPage.add(pid);
            			//System.out.println("end exclusive");
            			return;
            		}
            		else if(getSharedLockList(pid).size()==1 & getSharedLockList(pid).get(0)==tid)
            		{
            			getSharedLockList(pid).clear();
            			exclusiveLocks.put(pid, tid);
            			getLockSet(tid).add(pid);
            			return;
            		
            		}
            	}
            }
        }
    }
    
    public boolean holdsLock(TransactionId tid, PageId pid)
    {
    	return getLockSet(tid).contains(pid);
    }
    
}
