package simpledb.storage;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private final PageId pageId;
    private final int tupleNumber;

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this.pageId=pid;
        this.tupleNumber=tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        return this.tupleNumber;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
       return this.pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        if(o==null)
            return false;
        RecordId recordId;
        if(o instanceof RecordId){
            recordId=(RecordId) o;
        }else {
            return false;
        }
        if(recordId.pageId.equals(this.pageId) && recordId.tupleNumber==this.tupleNumber)
            return true;
        return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        return (String.valueOf(this.pageId.getTableId())+";"+String.valueOf(this.pageId.getPageNumber())+":"+String.valueOf(this.tupleNumber)).hashCode();
    }

}
