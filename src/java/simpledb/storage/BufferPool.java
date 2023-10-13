package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.Lock;
import simpledb.transaction.LockType;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.HashMap;
import java.util.Map;
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
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int maxPages;

    private final HashMap<Integer,Page> bufferPool;//缓冲池由很多页面组成，每个页面只能存储一个磁盘加载的页面；为什么不用list或者数组呢，因为利用哈希表可以加快数据页面在buffer pool中的定位，而不需要线性时间

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.bufferPool = new HashMap<>(numPages);//在物理内存中申请一块可以容纳numPages个数据页的空间
        maxPages=numPages;
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.；如果缓冲池中没有足够的空间，则应驱逐页面并添加新页面来代替它。
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        Page page;
        if(this.bufferPool.containsKey(pid.hashCode())){
            page=this.bufferPool.get(pid.hashCode());
            if(perm==Permissions.READ_ONLY) {
                if(!holdsLock(tid,pid))
                    Database.getLockManager().getReadLock(page.getId(), tid);
                else// 如果有锁，肯定至少可以满足读的权限，不需要再次申请锁
                    ;//do nothing
            } else {
                if(!holdsWriteLock(tid, pid))
                    Database.getLockManager().getWriteLock(page.getId(), tid);
            }
            return page;
        }
        if(bufferPool.size()>=maxPages){
            evictPage();
        }
        int tableId = pid.getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        page = file.readPage(pid);
        //buffer pool没有足够的空间了
        this.bufferPool.put(pid.hashCode(), page);
        if(perm==Permissions.READ_ONLY) {
            if(!holdsLock(tid,pid))
                Database.getLockManager().getReadLock(page.getId(), tid);
            else// 如果有锁，肯定至少可以满足读的权限，不需要再次申请锁；为什么新载入buffer pool的page也可能有锁呢，因为有锁但是没有被修改页面可能被evict驱逐
                ;//do nothing
        }else {
            if(!holdsWriteLock(tid, pid))
                Database.getLockManager().getWriteLock(page.getId(), tid);
            else
                ;//do nothing
        }

        return page;
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        if(!holdsLock(tid, pid))
            ;//do nothing
        else if(holdsWriteLock(tid, pid)){
            Database.getLockManager().releaseWriteLock(pid, tid);
        }else {
            Database.getLockManager().releaseReadLock(pid, tid);
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        boolean hold=false;
        for (int i = 0; i < tid.occupiedLocks.size(); i++) {
            Lock lock=tid.occupiedLocks.get(i);
            if(lock.getPageId().equals(p)){
                hold=true;
                break;
            }
        }
        return hold;
    }

    public boolean holdsWriteLock(TransactionId tid, PageId p) {
        boolean hold=false;
        for (int i = 0; i < tid.occupiedLocks.size(); i++) {
            Lock lock=tid.occupiedLocks.get(i);
            if(lock.getPageId().equals(p) && lock.getLockType()== LockType.Write){
                hold=true;
                break;
            }
        }
        return hold;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // 一开始莫名其妙将if commit这段代码放在while循环后面。。。。。 那样的话flushPages一个页面也没有flush到磁盘，因为flushPages是通过occupiedLocks查看占用的页面的，occupiedLocks都释放了，flushPages以为这个事务一个页面都没有占用。。。。。。
        // 上面说的这个错误在TransactionTest.testTwoThreads()中是测试不出来的，因为该测试中一个事务成功后就算flush失败，另一个事务也能读取到buffer pool中更新的值
        // 在testTenThreads()或者testFiveThreads()中，一个事务成功后，flush失败，并没有将更改持久化到磁盘，然后对2个测试分别可能会有剩下的9个或者4个事务开始读，然后都失败了，依此abort，直到最后一个事务abort，于是他们占用共享锁的页面可以discard掉（此时第一个事务write的值已经丢失了，discard就是简单从bufferPool map把page remove掉），因为最后一个事务释放锁时已经独占共享锁，可以discard；接着又有新的事务加入，加入的第一个事务发现page不在buffer pool中，于是从磁盘中加载，由于第一个事务write的值在的页面被discard了并没有持久化，所以此时从磁盘加载的值是丢失过修改的值。
        if(commit){// commit的情况需要flush脏页;
            try {
                flushPages(tid);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        while(tid.occupiedLocks.size()!=0){// 注意！这里不能for(int i=0;i<tid.occupiedLocks.size();i++) 因为occupiedLocks长度在经过一次循环后可能发生变化；int cnt=tid.occupiedLocks.size();循环里用cnt可以，但是不是每次gei(i)，而是get(0)；最好直接用while
            if(tid.occupiedLocks.get(0).getLockType()==LockType.Read)
                Database.getLockManager().releaseReadLock(tid.occupiedLocks.get(0).getPageId(), tid);
            else
                Database.getLockManager().releaseWriteLock(tid.occupiedLocks.get(0).getPageId(), tid);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // 用于测试的输出代码
//        if(t.getField(0) instanceof IntField){
//            System.out.println("事务"+tid.getId()+" 想要insert的值："+((IntField) t.getField(0)).getValue());
//        }
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        file.insertTuple(tid, t);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // 用于测试的输出代码
//        if(t.getField(0) instanceof IntField){
//            System.out.println("事务"+tid.getId()+" 想要delete的值："+((IntField) t.getField(0)).getValue());
//        }
        int tableId=t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        file.deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Map.Entry<Integer, Page> entry : bufferPool.entrySet()) {
            flushPage(entry.getValue().getId());
        }
        // some code goes here
        // not necessary for lab1

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        System.out.println("remove page");
        this.bufferPool.remove(pid.hashCode());
        System.out.println("buffer pool page num: "+this.bufferPool.size());
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        int tableId = pid.getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        file.writePage(this.bufferPool.get(pid.hashCode()));
        this.bufferPool.get(pid.hashCode()).markDirty(false, new TransactionId());
        // some code goes here
        // not necessary for lab1
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        System.out.println("flush size:"+tid.occupiedLocks.size());
        for (int i = 0; i < tid.occupiedLocks.size(); i++) {
            PageId pageId=tid.occupiedLocks.get(i).getPageId();
            Page page;
            if(this.bufferPool.containsKey(pageId.hashCode())){
                System.out.println("flush after write commit");
                page=this.bufferPool.get(pageId.hashCode());
                if(page.isDirty()!=null){// todo 需不需要判断修改该页面的transactionId是哪个
                    flushPage(pageId);
                }
            }else {
                ;//do nothing 因为现在的简单驱逐策略中 会驱逐加了读锁或写锁，但是页面并没有发生修改的页面
            }
        }
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // 用于测试
        System.out.println("evict");

        boolean allDirtied=true;
        //这里使用一个最简单的驱逐策略，找到第一个clean page，不管这个clean page上面有没有锁
        for (Map.Entry<Integer, Page> entry : bufferPool.entrySet()) {
            try {
                if(entry.getValue().isDirty()==null){
                    flushPage(entry.getValue().getId());// 这一句其实是多余的，因为没有发生修改，所以不需要flush
                    // todo 怎么处理释放的页面上已经存在的锁 假设有锁的不能驱逐
                    if(!Database.getLockManager().pageLocked(entry.getValue().getId())){
                        discardPage(entry.getValue().getId());
                    }
                    allDirtied=false;
                    break;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if(allDirtied)
            throw new DbException("all page in buffer pool dirtied");
        // some code goes here
        // not necessary for lab1
    }

}
