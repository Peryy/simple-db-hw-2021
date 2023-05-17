package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.lang.model.type.ArrayType;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private final File file;
    private final TupleDesc td;

    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td =td;
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
     * you will need to generate this tableid somewhere to ensure that each
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
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageSize = BufferPool.getPageSize();
        int pageNo = pid.getPageNumber();
        int offset = pageSize * pageNo;
        Page page = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "r");
            byte[] data = new byte[pageSize];
            raf.seek(offset);
            raf.read(data);
            page = new HeapPage(((HeapPageId) pid), data);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                raf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        long fileLength = file.length();
        return (int) Math.ceil((double) fileLength / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }
    public static final class HeapFileIterator implements DbFileIterator {

        private TransactionId tid;
        private Iterator<Tuple> iterator;
        private HeapFile heapfile;
        private int pageNo=0;

        public HeapFileIterator(HeapFile file, TransactionId tid) {
            this.heapfile = file;
            this.tid = tid;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            HeapPageId pageID = new HeapPageId(heapfile.getId(), pageNo);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageID, Permissions.READ_ONLY);
            if (page == null){
                throw new DbException("page is not existed");
            }
            iterator = page.iterator();

        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (iterator == null){
                return false;
            }
            if (!iterator.hasNext()) {

                pageNo++;
                if (pageNo >= heapfile.numPages()) {
                    return false;
                }
                HeapPageId pageID = new HeapPageId(heapfile.getId(), pageNo);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageID, Permissions.READ_ONLY);
                if (page == null){
                    throw new DbException("page is not existed");
                }
                return true;

            } else {
                return true;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!hasNext()){
                throw new NoSuchElementException("This is the last element of Iterator");
            }
            if (!iterator.hasNext()) {
                HeapPageId pageID = new HeapPageId(heapfile.getId(), pageNo);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageID, Permissions.READ_ONLY);
                iterator = page.iterator();
            }
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
            iterator = null;
        }
    }
}

