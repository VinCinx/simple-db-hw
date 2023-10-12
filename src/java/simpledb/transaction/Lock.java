package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.ArrayList;

// 升级或者降级锁的方法一般不会是使用，一般都是先释放再获得
public class Lock {
    private LockType lockType;// 锁类型

    private final PageId pageId;

    private ArrayList<TransactionId> lockTransactionIdList = new ArrayList<>();

    Lock(LockType lockType, PageId pageId) {
        this.lockType = lockType;
        this.pageId=pageId;
    }

    /**
     * 只有在LockType是Read的时候才能在当前锁上继续加事务
     *
     * @param transactionId
     * @return
     */
    public synchronized boolean addLock(TransactionId transactionId) {
        if (lockType == LockType.Read) {// 这是一个读锁
            if(lockTransactionIdList.contains(transactionId))// 该事务已经申请了这个读锁，防御式编程
                return true;
            lockTransactionIdList.add(transactionId);
            transactionId.occupiedLocks.add(this);
            return true;
        }else {
            if(lockTransactionIdList.size()!=0){
                throw new RuntimeException("writes are exclusive");
            }
            lockTransactionIdList.add(transactionId);
            transactionId.occupiedLocks.add(this);
            return true;
        }
    }

    public synchronized LockStatus releaseLock(TransactionId transactionId) {
        lockTransactionIdList.remove(transactionId);
        transactionId.occupiedLocks.remove(this);
        if (lockType == LockType.Write)
            return LockStatus.Released;
        if (lockTransactionIdList.size() == 0) {
            return LockStatus.Released;
        } else {
            return LockStatus.Occupying;
        }
    }

    public PageId getPageId(){
        return pageId;
    }

    /**
     * 因为可能锁升级，所以加上同步
     * @return
     */
    public synchronized LockType getLockType() {
        return lockType;
    }

    public synchronized  ArrayList<TransactionId> getLockTransactionIdList(){
        return lockTransactionIdList;
    }

    public synchronized void upgradeReadLock(){// 这种锁升级方式可能不太恰当，理论上升级时先丢弃读锁再加写锁更加合适，不过两者在此lab中行为一致，这种直接升级锁的方式能正确运行
        lockType=LockType.Write;
    }

    public synchronized boolean occupyingLockAlone(TransactionId transactionId){
        if(lockTransactionIdList.contains(transactionId) && lockTransactionIdList.size()==1) // alone，添加==1的判断
            return true;
        return false;
    }

    public synchronized boolean isOccupying(TransactionId transactionId){
        return lockTransactionIdList.contains(transactionId);
    }
}
