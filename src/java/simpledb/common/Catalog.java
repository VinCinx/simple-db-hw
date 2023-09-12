package simpledb.common;

import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

    //Integer是file的id，id使用的是file文件名的hashcode
    private final Map<Integer, DbFile> dbfiles;//这种方式在文件很大的时候太占用内存，实际不是这样吧? 不是这里DbFile可能存储的只是一个指针罢了
    private final Map<Integer, String> tableNames;
    private final Map<Integer, String> pkeyFieldNames;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        dbfiles=new HashMap<>();
        tableNames=new HashMap<>();
        pkeyFieldNames=new HashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // 这里将使用一个class包裹一下，精简成只用一个hashmap更好，原来我是这么想的，觉得实验中可能尽量不要新建辅助类，所以改成这样。。
        // 做到lab1 exercise5 实现HeapFile的iterator的时候证明我多虑了，新建类很正常。
        dbfiles.put(file.getId(), file);
        tableNames.put(file.getId(), name);
        pkeyFieldNames.put(file.getId(), pkeyField);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        for (Integer id: tableNames.keySet()) {
            if (tableNames.get(id).equals(name)) {
                return id;
            }
        }
       throw new NoSuchElementException();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        if(dbfiles.containsKey(tableid)){
            DbFile file = dbfiles.get(tableid);
            return file.getTupleDesc();
        }
        throw new NoSuchElementException();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        if(dbfiles.containsKey(tableid))
            return dbfiles.get(tableid);
        throw new NoSuchElementException();
    }

    public String getPrimaryKey(int tableid) {
        if(pkeyFieldNames.containsKey(tableid))
            return pkeyFieldNames.get(tableid);
        throw new NoSuchElementException();
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return null;
    }

    public String getTableName(int id) {
        if(tableNames.containsKey(id))
            return tableNames.get(id);
        // some code goes here
        return null;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        this.dbfiles.clear();
        this.tableNames.clear();
        this.pkeyFieldNames.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

