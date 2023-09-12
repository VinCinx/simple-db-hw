# mit 6.830

相比cmu15445可能文档不够完善，需要结合一定的看课程视频学习，不过该实验还是很主流的。

实验环境

- 建立私人仓库，将代码clone下来并与个人github的私人仓库关联
- clone代码后，设置jdk和project structure，设置好项目的根目录，这样java文件从黄变绿，java文件开头的package后面跟的文件名一般为当前文件的相对路径，设置根目录就是告诉它在相对路径前面加上根目录寻址；分别设置主文件目录和test文件目录的根目录
- 解决上述问题后junit仍然会报错，因为该项目直接在项目中使用junit的jar包，而不是利用管理工具从外部导入，右键lib文件夹，选择add as a library
- brew update; brew install ant，在mac上安装ant
- ant的build.xml文件爆红，直接点击右侧的runtest会报错，要求调用该命令的时候制定待运行测试文件的文件名
- 右键运行某一个测试文件，报错“不支持发行版本 20”，要在settings吧java compiler改成11（与所使用的jdk版本对应），在project structure里把language level页设置为与jdk对应的版本，再次运行单个测试文件，不报错

# **Lab1**

在上手实验之前，阅读了一部分数据库书籍，对数据库的体系有基本的了解，所以直接开始阅读实验文档、编码。

方法：面向测试用例编程

lab地址：https://github.com/MIT-DB-Class/simple-db-hw-2021

用时：2day（断断续续的时间）

注意点：认真阅读代码中的注释提示，对理解代码骨架、编写待填充部分很有帮助

### **Exercise 1**

比较容易，实现TupleDesc、Tuple类代码

TupleDesc用于表示表头信息，包括有多少列，每一列的字段名、字段类型

Tuple则表示表中每一行数据，存放每行数据具体的字段值

### **Exercise 2**

同样比较简单，实现Catalog，Catalog中记录了数据库中所有表以及它们的schema信息（dbfiles-用于管理表对应的磁盘文件；tableNames-表名；pkeyFieldNames主键名）

使用Map实现，key为dbfile的id（该id使用dbfile对应磁盘文件的文件路径哈希值表示），一张表对应一个磁盘文件、一个dbfile，所以这里的id就是表id（PageId结构体中的tableId与之对应）。

```java
public class Catalog {

    //Integer是file的id，id使用的是file文件名的hashcode
    private final Map<Integer, DbFile> dbfiles;//这种方式在文件很大的时候太占用内存，实际不是这样吧? 不是这里DbFile可能存储的只是一个指针罢了
    private final Map<Integer, String> tableNames;
    private final Map<Integer, String> pkeyFieldNames;
		...
```

或者新建一个类包含所有schema信息，这样只需要使用一个Map

### **Exercise 3**

实现BufferPool中的`getPage(TransactionId tid, PageId pid, Permissions perm)`方法，该方法负责从Buffer pool中读取某个页面，如果发现不在Buffer pool中，则拿到该页面对应的DbFile，调用`DbFIle.readPage()`方法将该页面从磁盘加载到Buffer pool

如何拿到`PageId`对应页面的DbFile：PageId结构体中存储了tableId，通过tableId到Catalog中查找DbFile

BufferPool使用什么样的结构：BufferPool缓存页面，也可以使用Map实现，<页面id，页面>；页面Id不能使用PageId中的pgNo，在HeapPageId中，对于每一张表，pgNo都是从0开始的（这一点从**Exercise 5**中可以得知），所以在PageId中实现一个`hashcode()` 方法，计算一个哈希值当作Map<页面id，页面>结构中的页面id。

## **Exercise 4**

完成HeapPageId、HeapPage、RecordId代码，这是重点

每个Tuple（存储一行记录值）中都有一个RecordId；

HeapPageId比较简单，加几行代码就行；

HeapPage中实现getNumEmptySlots()和isSlotUsed()方法，可以参考src/simpledb/HeapFileEncoder.java中的代码理解一个Page的结构是什么样的，相关内容在实验文档中介绍得很清晰。

难点、易错点：

为HeapPage实现一个Iterator，用于查看HeapPage中的每个tuple，可能用到辅助类或数据结构。这里我一开始只是简单地计算出已经占用的Slot数量，然后截取HeapPage中的Tuple[]，改为ArrayList后调用ArrayList自带的iterator()方法返回，测试并不能测出这样写的问题；实际上记录在页面中不一定连续存放，也就是Tuple[]中可能Tuple[0]存放一条记录的数据值，Tuple[1]为空，Tuple[2]存放一条记录的数据值…，所以将代码更改为遍历Tuple[]，每次便利根据该Tuple对应的Slot判断要不要加入ArrayList。

没有引入辅助类自己实现一个Iterator，不知道在后续实验中会不会出问题。

## **Exercise 5**

难点

实现HeapFile中的readPage(PageId pid)方法，支持随机访问；为HeapFile实现一个迭代器，用于读取HeapFile对应表中的页面，在open后才支持访问，在close后不支持hasNext()、next()等访问，open时不应该一次性将HeapFile对应表的所有页面都加载到内存，因为这样的话在表表很大时会发生oom错误

readPage(PageId pid)实现相对容易，根据PageId的pgNo计算一个偏移量，使用RandomAccessFile类从文件偏移量开始读取一个Page大小的byte，利用该byte数组构造一个Page对象；

HeapFile比较难，可以参照AbstractDbFileIterator编写，由于不能一次性将所有页面的所有Tuple加载到内存，所以一次只加载一个页面的所有Tuple，需要添加一个pgNo相关的变量用于记录读到了第几个页面。

## **Exercise 6**

水到渠成，借助前面实现的内容实现SeqScan，用于一个事务中读取文件，利用HeapFileIterator就行了。注意reset(int tableid, String tableAlias)方法，不仅仅是修改这两个SeqScan结构中变量，reset的含义是为事务重新设置一个要读的表，相应的HeapFileIterator等内容也要改变。