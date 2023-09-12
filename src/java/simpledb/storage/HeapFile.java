package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

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
public class HeapFile implements DbFile{

    private final File file;
    private final TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file=f;
        this.td=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
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
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        Page page = null;
        try {
            RandomAccessFile raf = new RandomAccessFile(this.file, "r");
            long offset = (long) BufferPool.getPageSize() *pid.getPageNumber();//raf.seek()接受的参数为long类型
            raf.seek(offset);
            byte[] data = new byte[BufferPool.getPageSize()];
            raf.read(data);
            raf.close();
            if(pid instanceof HeapPageId){
                page = new HeapPage((HeapPageId)pid, data);
            }
        }catch (IOException e){
            e.printStackTrace();
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
     * ；这里，写到lab1 Exercise5的时候卡住了
     * 我暂时没有看视频，面向测试和说明文档编程，很不巧的是这两个文件并不能提供足够多的信息，所以我选择看一部分别人对于实验的理解，当然一般情况不看代码（用到不熟悉IO函数的时候，想出解决思路后适当参考，因为实验是理解DB，语言次要）
     * 比如lab1写到这里，看了一个别人的文章后update了我对HeapPageId里pgNo的理解：
     * 对于HeapPageId对应的表的磁盘文件，其中有n个页面，从文件开始读取页面，这些页面的pgNo依次递增，我一开始认为pgNo是全局的，现在看来确实没必要。
     */
    public int numPages() {
        return (int)file.length()/BufferPool.getPageSize();
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
        return new HeapFileIterator(tid, this);
    }
}

