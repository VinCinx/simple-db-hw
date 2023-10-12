# Lab3 **Query Optimization**

用时：2day

在做完lab2后lab3暂时搁置了两天，中间去看了一次五月天的演唱会💐，由于是人生的第一次演唱会，所以比较激动，所以看完后的一天心情有点难以平复，好在终于2天内完成了这个lab。

此lab主要实现查询优化功能，难点在于文档的阅读和细节的把控，建议上手之前通读一篇文档，理解专业术语后从头开始阅读并实现。读懂了文档之后就非常简单了，只是需要注意一些数值的边界条件，因为做优化嘛，会涉及到一些开销等统计数值的比较。

在做这个实验之前了解到这一套lab的文档并不全面，需要辅以视频和书，不像某些实验只要理解文档就可以实现lab，但是对于这个lab3不是这样的，理解文档并实现就可以通过全部测试用例。

## Exercise 1

计算Filter Selectivity，利用直方图进行概率统计，可以使用固定数量的buckets进行统计，注意考虑负数情况、每个bucket的长度、bucket的左右边界；使用定长buckets进行统计时，注意最后一个bucket的有效长度可能比前面桶的长度短，因为min到max之间的整数个数并不一定能恰好整除设定的buckets个数。

buckets的数量不要过少，使用一个桶统计在区分度上没有什么意义。

## Exercise 2

计算单表的一些统计数据，比较简单。最主要实现estimateSelectivity()函数，实现此函数的基础是完善构造函数TableStats()，在该函数中利用Exercise 1实现的直方图收集表中每一列数据的信息，在这之后estimateSelectivity()通过调用**Histogram的方法即可实现。

## Exercise 3

实现estimateJoinCost()和estimateJoinCardinality()两个函数，对于实现要求，文档中分别对于join的cost、cardinality如何计算进行了说明。

## Exercise 4

计算优化后的查询计划，难点在于理解文档中给出的伪代码，如何使用PlanCache结构体实现伪代码中bestPlan的功能。



bonus to update，做本次实验的主要目的不是学习数据库的Query Optimization部分，所以bonus部分暂时不做。

lab2新涉及类、方法和对象的图表

![lab3](lab3-writeup.assets/lab3.png)





**至此完成了前3个lab，简单回顾一下：** 

lab1是上手的第一个实验，需要理解项目骨架，理解内存与磁盘交互的相关知识，难度适中；

Lab2基于lab1，实现operators，需要十分注意对于buffer pool的使用，各个类之间的层级结构，与lab1难度相当，代码量比较大；

lab3的4个exercise难度相对而言整体不大，理解文档即可，代码量也比前两个lab少。