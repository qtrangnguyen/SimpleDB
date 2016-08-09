package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc tupledesc;
    

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupledesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupledesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
         // create a new RandomAccessFile with filename test
         RandomAccessFile raf = new RandomAccessFile(file, "r");
         int offset = pid.pageNumber()*Database.getBufferPool().getPageSize();
         byte[] b=new byte[Database.getBufferPool().getPageSize()];
         raf.seek(offset);
         raf.read(b, 0, Database.getBufferPool().getPageSize());
         HeapPageId hpid=(HeapPageId)pid;
         raf.close();  
         return new HeapPage(hpid, b);

      } catch (IOException ex) {
         ex.printStackTrace();
      }
      return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        try {
         // create a new RandomAccessFile with filename test
         RandomAccessFile raf = new RandomAccessFile(file, "rw");
         PageId pid = page.getId();
         int offset = pid.pageNumber()*Database.getBufferPool().getPageSize();
         byte[] b = page.getPageData();
         raf.seek(offset);
         raf.write(b, 0, Database.getBufferPool().getPageSize());
         raf.close();  
      } catch (IOException ex) {
         ex.printStackTrace();
      }

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)Math.ceil(file.length()/Database.getBufferPool().getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> arr = new ArrayList<Page>();  
        int tableid=this.getId();
        for (int i=0; i<this.numPages();i++){
            HeapPageId pid= new HeapPageId(tableid,i);
            Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (((HeapPage)page).getNumEmptySlots()!=0){
                arr.add(page);
                break;
            }
        }
        
        if(arr.size()==0)
        {
            HeapPageId hpid=new HeapPageId(this.getId(), this.numPages());
            HeapPage hp=new HeapPage(hpid, HeapPage.createEmptyPageData());
            hp.insertTuple(t);
            this.writePage(hp);
            arr.add(hp); 
        }        
        else
        {
            Page p = arr.get(0);
            HeapPage hp=(HeapPage)p;
            hp.insertTuple(t);
            arr.add(hp);
        }

        return arr;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage heappage = (HeapPage)Database.getBufferPool().getPage(tid,t.getRecordId().getPageId(),Permissions.READ_WRITE);
        heappage.deleteTuple(t);
        return heappage;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        DbFileIterator it = new HeapFileIterator(tid, this);
        return it;
    }

}

