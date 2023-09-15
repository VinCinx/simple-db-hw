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
        try {
            RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
            long offset = (long) BufferPool.getPageSize() *page.getId().getPageNumber();//raf.seek()接受的参数为long类型
            raf.seek(offset);
            byte[] data = page.getPageData();//todo 这里需不需要强制转换为HeapPage
            raf.write(data);
            raf.close();
        }catch (IOException e){
            e.printStackTrace();
        }
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
        for (int i = 0; i < numPages(); i++) {//pgNo从0开始
            Page page=Database.getBufferPool().getPage(tid,new HeapPageId(getId(), i),Permissions.READ_WRITE);
            HeapPage heapPage=(HeapPage) page;
            int numEmptySlots=heapPage.getNumEmptySlots();
            if(heapPage.getNumEmptySlots()==0)
                continue;
            else {
                heapPage.insertTuple(t);
                page.markDirty(true,tid);
                ArrayList<Page> pageArrayList=new ArrayList<>();
                pageArrayList.add(page);
                return pageArrayList;
            }
        }
        // 两种处理：1.将新建并insert一个tuple的page写入文件 下次使用再加入缓存 不太合理 因为修改都在缓存 而且这样这里不需要标记脏页 不统一；先新建、insert，再写入磁盘，再加入缓存，不用标记脏页
        // 2.新建页面写入磁盘，立即加入缓存修改，并标记为脏页
        // 没有空闲的页面
        byte[] data=HeapPage.createEmptyPageData();
        HeapPageId heapPageId=new HeapPageId(getId(),numPages());//加载到缓存的时候要用，加载到缓存不能再new 一个，numPages()再writePage()之后增加了
        HeapPage heapPage=new HeapPage(heapPageId,data);
        // 写入磁盘
        writePage(heapPage);
        // 加载到缓存并修改
        Page page=Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_WRITE);
        ((HeapPage)page).insertTuple(t);
        page.markDirty(true,tid);
        // 上面这个过程，如果先将空页面写入磁盘，然后不加到缓存就修改用byte[]新建的空的heapPage，下次缓存读的是磁盘的空页，第一次修改没有被缓存结构管理，会丢失
        ArrayList<Page> pageArrayList=new ArrayList<>();
        pageArrayList.add(page);
        return pageArrayList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        PageId pageId=t.getRecordId().getPageId();
        Page page=Database.getBufferPool().getPage(tid,pageId,Permissions.READ_WRITE);
        if(page instanceof HeapPage){
            ((HeapPage)page).deleteTuple(t);
            ArrayList<Page> pageArrayList=new ArrayList<>();
            pageArrayList.add(page);
            return pageArrayList;
        }else {
            throw new DbException("pageis not instanceof HeapPage");
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }
}

