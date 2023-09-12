package simpledb.storage;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    private final int tableId;

    private final int pgNo;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        this.tableId=tableId;
        this.pgNo=pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        return this.tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
        return this.pgNo;
    }

    /**
     * @return a hash code for this page, represented by a combination of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        String str=String.valueOf(this.tableId)+";"+String.valueOf(this.pgNo);
        return str.hashCode();//table number和page number之间加一个分隔符，能更好的进行唯一区分
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        if(o==null)
            return false;
        HeapPageId heapPageId;
        if(o instanceof HeapPageId){
            heapPageId=(HeapPageId) o;
        }else {
            return false;
        }
        if(heapPageId.tableId==this.tableId && heapPageId.pgNo==this.pgNo)
            return true;
        return false;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int[] data = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
