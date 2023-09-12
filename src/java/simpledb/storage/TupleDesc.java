package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.；描述的一张表的表头信息，即有哪些字段
 */
public class TupleDesc implements Serializable {

    private TDItem[] tdItems;

    /**
     * A help class to facilitate organizing the information of each field；描述了某一列的字段名+字段类型
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        int colNum = typeAr.length;
        this.tdItems = new TDItem[colNum];
        for(int i=0; i<colNum; ++i){
            TDItem tdItem = new TDItem(typeAr[i], fieldAr[i]);
            tdItems[i] = tdItem;
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        int colNum = typeAr.length;
        this.tdItems = new TDItem[colNum];
        for(int i=0; i<colNum; ++i){
            TDItem tdItem = new TDItem(typeAr[i], null);// 匿名的列
            tdItems[i] = tdItem;
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if(i<0 || i>this.numFields()-1)
            throw new NoSuchElementException();
        return this.tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if(i<0 || i>this.numFields()-1)
            throw new NoSuchElementException();
        return this.tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        int index = -1;
        for(int i=0; i<this.numFields(); ++i){
            String filedName = this.getFieldName(i);//fieldName可能是null
            if(filedName!=null && filedName.equals(name)){
                index = i;
                break;
            }
        }
        if(index<0)
            throw new NoSuchElementException();
        return index;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.；实验中涉及的数据类型都是定长的
     */
    public int getSize() {
        int size = 0;
        for (TDItem tdItem : tdItems) {
            size += tdItem.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        Type[] typeAr = new Type[td1.numFields()+td2.numFields()];
        String[] fieldAr = new String[td1.numFields()+td2.numFields()];
        int pos = 0;
        for(int i=0; i<td1.numFields(); i++){
            typeAr[pos]=td1.getFieldType(i);
            fieldAr[pos]=td1.getFieldName(i);
            pos++;
        }
        for (int i = 0; i < td2.numFields(); i++) {
            typeAr[pos]=td2.getFieldType(i);
            fieldAr[pos]=td2.getFieldName(i);
            pos++;
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if(o==null)
            return false;
        TupleDesc tupleDesc;
        if(o instanceof TupleDesc)
            tupleDesc = ((TupleDesc)o);
        else
            return false;
        if(this.numFields()!=tupleDesc.numFields())
            return false;
        for(int i=0; i<this.numFields(); i++){
            if(this.getFieldType(i)!=tupleDesc.getFieldType(i))
                return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String tupleDescription = "";
        for (int i = 0; i < this.numFields(); i++) {
            String fieldDescription = "";
            fieldDescription+=this.getFieldType(i)+"("+this.getFieldName(i)+")";
            if(i!=this.numFields())
                tupleDescription+=fieldDescription+",";
        }
        return tupleDescription;
    }
}
