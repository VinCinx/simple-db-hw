package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header;
    final Tuple[] tuples;
    final int numSlots;

    private boolean dirty;
    private TransactionId transactionId;//transactionId of the transaction which dirtied this page

    byte[] oldData;
    private final Byte oldDataLock= (byte) 0;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page；该页面可以容纳的页面数量，而非已经占用的页面数量
    */
    private int getNumTuples() {
        int nrecbytes = 0;
        for(int i=0; i<this.td.numFields(); i++){
            nrecbytes+=td.getFieldType(i).getLen();
        }
        int nrecords = (BufferPool.getPageSize() * 8) /  (nrecbytes * 8 + 1);  //floor comes for free
        return nrecords;
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        int nheaderbytes = (this.numSlots / 8);
        if (nheaderbytes * 8 < this.numSlots)
            nheaderbytes++;  //ceiling
        return nheaderbytes;
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // 用于测试
//        System.out.println(getNumEmptySlots()+" "+((IntField)t.getField(0)).getValue());
        for (int i = 0; i < tuples.length; i++) {
            if(!isSlotUsed(i)){//如果没有这个判断直接进入下面的判断，tuples[i]可能是没有内容的，会NullPointerException
                continue;
            }
            if(t.getRecordId().equals(tuples[i].getRecordId())){
                markSlotUsed(i, false);
                // 用于测试
//                System.out.println(getNumEmptySlots()+" "+((IntField)t.getField(0)).getValue());
                return;
            }
        }
        throw new DbException("this tuple is not on this page");
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        if(!t.getTupleDesc().equals(td))
            throw new DbException("tupledesc is mismatch");
        if(getNumEmptySlots()==0)
            throw new DbException("the page is full (no empty slots)");
        boolean success=false;
        for (int i = 0; i < this.numSlots; i++) {
            if(!this.isSlotUsed(i)) {
                //更新插入的Tuple的RecordId信息
                int tupleNumber=i;
                RecordId recordId=new RecordId(pid,tupleNumber);
                t.setRecordId(recordId);
                tuples[i] = t;
                markSlotUsed(i, true);//标记为已占用并break
                success=true;
                break;
            }
        }
        if(!success)
            throw new DbException("the page is full (no empty slots)");
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        this.dirty=dirty;
        this.transactionId=tid;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        if(dirty){
            return transactionId;
        }
        return null;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        int usedSlotNum = 0;
        for (int i = 0; i < header.length; i++) {//注意，如果对byte边右移边统计1的个数，不能以byte==0为循环终止条件，因为逻辑右移是将byte扩展为32位int然后右移，如果是8个1，右移8次后byte仍然不为0
            for (int j = 0; j < 8; j++) {
                if((this.header[i] & (1<<j))!=0){//不能用==1作为判断条件，1不在最右边最低位的时候，左边的值不等于1
                    usedSlotNum++;
                }
            }
        }
        return this.numSlots-usedSlotNum;
    }


    public String byteToBinaryString(byte value) {
        StringBuilder binaryString = new StringBuilder(8);
        for (int i = 7; i >= 0; i--) {
            int bit = (value >> i) & 1;
            binaryString.append(bit);
        }
        return binaryString.toString();
    }

    /**
     * Returns true if associated slot on this page is filled.
     * 这里的i也是从0开始编号
     */
    public boolean isSlotUsed(int i) {
        int headerPos = i/8;
        byte n = this.header[headerPos];
        int bytePos = i%8;
        int judge = (n>>>bytePos) & 1;
        return judge == 1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     * i定义为从0开始，使用的时候注意一下
     */
    private void markSlotUsed(int i, boolean value) {
        int headerPos = i/8;
        byte n = this.header[headerPos];
        int bytePos = i%8;
        if(value){
            this.header[headerPos] = (byte)(n | 1<<bytePos);
        }else {
            this.header[headerPos] = (byte)(n & ~(1<<bytePos));
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // 用于测试
//        System.out.println("获得iterator时的空闲slot数量"+getNumEmptySlots()+" ");
        ArrayList<Tuple> usedTuples = new ArrayList<>();
        for (int i = 0; i < this.numSlots; i++) {
            if(this.isSlotUsed(i))
                usedTuples.add(this.tuples[i]);
        }
        // 用于测试
//        System.out.println("已经占用的tuple的数据内容"+Arrays.toString(usedTuples.toArray()));
        System.out.println();
        return usedTuples.iterator();
    }

}

