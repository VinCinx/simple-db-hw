package simpledb.transaction;

import simpledb.storage.HeapPageId;
import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本对象设置为一个单例，在Database中注册（与Catalog类似）
 * 对于普通同步方法，synchronized加锁是基于对象
 * 以页面为粒度加锁；一个事务申请加锁可能被阻塞，一个事务上只有可能阻塞一个锁，阻塞了就无法继续进行无法再次申请锁；
 * 一个锁上面可能阻塞了多个事务，多事务访问同一区域内容；
 * 一个事务释放了某个锁之后，可能会唤醒某个阻塞在这个锁上面的事务
 */
public class LockManager {

    // 这个时间不能太短，否则random随机数的作用降低，随机数不能有效区分wait时间，事务多了之后wait作用不明显，很可能运行非常长时间TransactionTest.testFiveThreads()这种才会有一个事务获得写锁
    // 增大TIMEOUT_MILLIS，同时random.nextInt随机数取值范围扩大之后，wait时间更加随机，同时由于TIMEOUT_MILLIS增大所以wait更有区分度——TransactionTest.testFiveThreads()能在不到30s内很快运行结束
    // 但是Expected :5
    // Actual   :2
    // 仍然存在问题
    private static final int TIMEOUT_MILLIS = 10;
    private Map<PageId, Lock> lockManager=new ConcurrentHashMap<>();

    private Random random = new Random();

    /**
     * 利用同步代码块保证加锁操作的原子性
     * @param pageId
     * @return
     */
    public synchronized boolean getReadLock(PageId pageId, TransactionId transactionId) throws TransactionAbortedException {
        // 如果当前页面上已经存在写锁，写锁也能读，直接返回true
        if(lockManager.containsKey(pageId) && lockManager.get(pageId).getLockType()==LockType.Write && lockManager.get(pageId).isOccupying(transactionId)){
            return true;
        }
        Lock lock;
        if(lockManager.containsKey(pageId) && lockManager.get(pageId).getLockType()==LockType.Read){
            lock=lockManager.get(pageId);
            lock.addLock(transactionId);
            return true;
        }
        // 1.当前页面上不存在锁 2.当前页面上存在锁，但是存在的是写锁，暂时不能加入读锁
        lock=new Lock(LockType.Read, pageId);
        boolean timeout=false;
        while (lockManager.containsKey(pageId)){// 因为当前方法获取了对象锁，而在这个方法里面无法移除lockManager中的某个key，所以使用wait()等改并释放锁
            if(timeout)
                throw new TransactionAbortedException();
            try {
                this.wait(TIMEOUT_MILLIS);
                timeout=true;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        lockManager.put(pageId,lock);
        lock.addLock(transactionId);
        return true;
    }

    public synchronized boolean releaseReadLock(PageId pageId, TransactionId transactionId){
        Lock lock;
        if(!lockManager.containsKey(pageId)){
            throw new RuntimeException("all type lock not exist in the page, the pageId {tableId: "+pageId.getTableId()+", pgNo: "+pageId.getPageNumber());
        }
        lock=lockManager.get(pageId);
        if(lock.getLockType()!=LockType.Read) {
//            throw new RuntimeException("read type lock not exist in the page");
            System.out.println("change release read lock");
            return true;// 写锁被升级为读锁，释放读锁直接返回，这里是一个特判断 todo
        }
        LockStatus lockStatus=lock.releaseLock(transactionId);
        System.out.println("release read lock, the pageId {tableId: "+pageId.getTableId()+", pgNo: "+pageId.getPageNumber());
        if(lockStatus==LockStatus.Released){
            lockManager.remove(pageId);
            notifyAll();
        }
        return true;
    }

    public synchronized boolean getWriteLock(PageId pageId, TransactionId transactionId) throws TransactionAbortedException {
        printLockManagerMap(transactionId,"point1");
        Lock lock;
        // 写锁和其他锁均互斥；特例：如果一个读锁只被一个事务持有，该事务想获得写锁，可能可以将该读锁直接升级为写锁
        if(lockManager.containsKey(pageId) && lockManager.get(pageId).getLockType()==LockType.Read && lockManager.get(pageId).occupyingLockAlone(transactionId)){
            lock=lockManager.get(pageId);
            lock.upgradeReadLock();
            printLockManagerMap(transactionId, "point3");
            return true;
        }
        // 该事务已经申请了这个读锁，防御式编程
        if (lockManager.containsKey(pageId) && lockManager.get(pageId).getLockType()==LockType.Write && lockManager.get(pageId).isOccupying(transactionId))
            return true;
        lock=new Lock(LockType.Write, pageId);
        long stopTestTime = System.currentTimeMillis() + TIMEOUT_MILLIS;
        boolean timeout=false;
        while (lockManager.containsKey(pageId) && !(lockManager.get(pageId).getLockType()==LockType.Read && lockManager.get(pageId).occupyingLockAlone(transactionId))){// 循环中需要再次考虑读锁升级成写锁的情况
            if(timeout)
                throw new TransactionAbortedException();
            try {
                // 如果想让等待的时间随机一点，TIMEOUT_MILLIS乘以一个随机数，随机数一定不能“可能为0”，否则有概率两个事务走到这里都取到0，那么都一直等待，不会超时失败，测试不通过：如果用random.nextInt()，则可以乘以(random.nextInt(2)+1)
                // 至于为什么要让等待的时间随机，如果等待时间一样，如果是事务A、B想要写锁，此时C想要写锁，则设置一样的等待时间，事务A、B差不多时间失败就可以，主要是下面的场景
                // 但是如果A、B都想通过升级读锁获得写锁，其中一个让步abort，预期是另一个升级成功，事实上如果wait的时间一样，A失败后还没来得及释放读锁，B就超过了wait时间，此时同样失败。因为A、B很可能开始wait的时间几乎一样，所以A throw new TransactionAbortedException()后，非常可能来不及释放锁B就超过等待时间，结果就是B也 throw new TransactionAbortedException()，与预期的B成功升级读锁不一样
                // 通过设置随机的wait时间能很大程度上解决这个问题（通过跑测试发现：wait时间一样时通过TransactionTest.testTwoThreads()需要上百个事务，中间会失败n次，wait时间随机后通过该测试只要几个事务）
                // （A想通过升级读锁获得写锁，B想直接获得写锁，B等待，A获得成功，很简单，没有前面的问题）
                this.wait((random.nextInt(5)+1)*TIMEOUT_MILLIS);
                timeout=true;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if(lockManager.containsKey(pageId) && lockManager.get(pageId).getLockType()==LockType.Read && lockManager.get(pageId).occupyingLockAlone(transactionId)){
            lock=lockManager.get(pageId);
            lock.upgradeReadLock();
            printLockManagerMap(transactionId, "point2");
            return true;
        }else {
            lockManager.put(pageId, lock);
            lock.addLock(transactionId);
            printLockManagerMap(transactionId,"point2");
            return true;
        }
    }

    public synchronized boolean releaseWriteLock(PageId pageId, TransactionId transactionId){
        Lock lock;
        if(!lockManager.containsKey(pageId)){
            throw new RuntimeException("all type lock not exist in the page");
        }
        lock=lockManager.get(pageId);
        if(lock.getLockType()!=LockType.Write)
            throw new RuntimeException("write type lock not exist in the page");
        LockStatus lockStatus=lock.releaseLock(transactionId);
        System.out.println("release write lock, the pageId {tableId: "+pageId.getTableId()+", pgNo: "+pageId.getPageNumber());
        if(lockStatus==LockStatus.Released){
            lockManager.remove(pageId);
            notifyAll();
        }
        return true;
    }

    /**
     * 判断某个页面上是否有锁
     */
    public synchronized boolean pageLocked(PageId pageId){
        return this.lockManager.containsKey(pageId);
    }

    /**
     * 判断某个页面上是否持有读锁
     */
    public synchronized boolean pageLockedByRead(PageId pageId){
        if(!this.lockManager.containsKey(pageId))
            return false;
        Lock lock=this.lockManager.get(pageId);
        if(lock.getLockType()==LockType.Read)
            return true;
        return false;
    }

    /**
     * 谨慎调用，目前只在buffer pool的重置方法里调用一次，因为锁的状态暂时可以理解为通过buffer pool管理
     */
    public synchronized void resetLockManager(){
        for (Map.Entry<PageId, Lock> entry : this.lockManager.entrySet()) {
            Lock lock=entry.getValue();
            ArrayList<TransactionId> transactionIdArrayList=lock.getLockTransactionIdList();
            for (int j = 0; j < transactionIdArrayList.size(); j++) {
                TransactionId transactionId=transactionIdArrayList.get(j);
                transactionId.occupiedLocks.clear();
            }
        }
        this.lockManager.clear();
    }

    /**
     * 打印lockManager的状态
     * 添加打印用于调试多线程：直接运行而不是debug，看打印的数据来看运行状态
     */
    public void printLockManagerMap(TransactionId transactionId, String position){
        System.out.println();
        System.out.println(position);
        System.out.println("事务id："+transactionId.getId());
        System.out.println("此时该事务占用的锁：");
        for (int i = 0; i < transactionId.occupiedLocks.size(); i++) {
            System.out.println("锁对应的tableId: "+transactionId.occupiedLocks.get(i).getPageId().getTableId()+", 锁对应的pgNo: "+transactionId.occupiedLocks.get(i).getPageId().getPageNumber()+", LockType: "+transactionId.occupiedLocks.get(i).getLockType());
        }
        System.out.println("此时的锁状态：");
        for (Map.Entry<PageId, Lock> entry : lockManager.entrySet()) {
            StringBuilder output= new StringBuilder("锁对应的tableId: "+entry.getKey().getTableId()+", 锁对应的pgNo: " + entry.getKey().getPageNumber() + ", LockType: " + entry.getValue().getLockType() + ", lockTransactionIdList: ");
            for (int i = 0; i < entry.getValue().getLockTransactionIdList().size(); i++) {
                output.append(entry.getValue().getLockTransactionIdList().get(i).getId()).append(" ");
            }
            System.out.println(output);
        }
        System.out.println();
    }
}

enum LockStatus{
    Released, Occupying
}
