Big Data - Batch Processing <br/> (with Hadoop-HBase)
===============================================

Introduction
----------------------------------------------
This repository contains implementations around Big Data and Batch Processing with use of Hadoop, MapReduce algorithm and HBase.

MapReduce was used to save and analyze Big Data sets.
HBase was used to insert and edit the prrevious data in order to be able to execute queries.
The data set that was used contained real user queries in Google Search Engine and a set containing the titles of all Wikipedia articles.

The file containing the queries was taken from the dataset released from [AOLSource](http://en.wikipedia.org/wiki/AOL_search_data_leak), with the exact files being available [here](https://github.com/dimosr7/Big_Data/tree/master/resources/user-ct-test-collection-01.txt.gz).
The file containing the titles of all Wikipedia articles was taken from [Wikipedia Source](dumps.wikimedia.org/enwiki/latest/enwiki-latest-all-titles-in-ns0.gz), with the exact files being available [here](https://github.com/dimosr7/Big_Data/tree/master/resources/enwiki-latest-all-titles-in-ns0.gz).

There are multiple parts/sub-projects in this repository

Map Reduce
--------------------------------------------------------
The following are implemented MapReduce jobs that were executed in a cloud environment containing 1 master and 2 slaves completing the tasks.
The purpose of each sub-project is described above:

1. Searches Sum Counting Per Day
A MapReduce algorithm was implemented in order analyse the dates on which the queries were executed.
The output was the dates with the number of searches executed during each date.
From the extracted data of MapReduce job, the following histogram had been designed
![alt text](https://github.com/dimosr7/Big_Data/blob/master/MapReduce/1/SearchCount/frequency_graph.png "Number of Queries for each Date")

2. Successfull and Unsuccessfull Search Sum 
A MapReduce algorithm was implemented in order to analyse the percentage of successfull and unsuccessfull queries. 
Successfull queries were those that resulted in selection of pages and the unsuccessfull queries were those that did not give any page as result

3. "Famous" Webpages
A MapReduce algorithm was implemented in order to analyse the most "visited" webpages. The output was a list, where the first column contained the URLs of webpages that were visited by more than 10 users and the second column contained those users. 

4. "Popular" Keywords in Searches
A MapReduce algorithm was implemented in order to analyse the most used keywords in queries. The output was a list, where the first column contained the 50 most searched keywords and the second column contained the number each keyword was used. The "noise" was removed by removing the unnecessary stop words, by using the following [list](http://jmlr.csail.mit.edu/papers/volume5/lewis04a/a11-smart-stop-list/english.stop) from MIT University, also available [here](https://github.com/dimosr7/Big_Data/tree/master/resources/english-stop.txt).

5. Histograms of Lexicographic Distribution.
A MapReduce algorithm was implemented in order to analyse the format of keywords and categorize them depending on the first letter. The output was a histogram of different categories of keywords depending on the starting letter. The Histogram program was used to create 3 histograms with different sample size : 50,1000 and all the Wikipedia titles. The Partitioner and Sorter programs were used to partition in almost same-sized categories the keywords and afterwards sort them alphabetically.
The produced graphs were the following :
![alt text](https://github.com/dimosr7/Big_Data/blob/master/MapReduce/5/Histogram/histogram50.png "50 most used keywords")
![alt text](https://github.com/dimosr7/Big_Data/blob/master/MapReduce/5/Histogram/histogram1000.png "1000 most used keywords")
![alt text](https://github.com/dimosr7/Big_Data/blob/master/MapReduce/5/Histogram/histogram_full.png "All keywords")

6. Wikipedia - resolvable queries
A MapReduce algorithm was implemented in order to analyse the queries that can be answered by Wikipedia. The output was the percentage of queries that contained at least a keyword that is contained in a Wikipedia article title.


HBase
------------------------------------------------------
The data extracted from the previous step of MapReduce jobs was inserted in HBase in order to maky dynamic queries and be able to analyse further the results.
The purpose of each sub-project is described above:

7. Wikipedia titles insertion in HBase
A MapReduce algorithm was implemented in order to insert all the Wikipedia titles in the HBase. For each title, an identifier was created by encrypting with MD-5 the title string. The table 'content' that was created contained the identifier and the title of the article.

8. Inverted Index 
A MapReduce algorithm was implemented in order to create an inverted index of records like keyword : {set of articles containing this keyword}. 
A new table 'index' was created from this MapReduce in the HBase, that contained the inverted index.

9. Queries Execution in HBase
A MapReduce algorithm was implemented in order to make random queries in the Hbase. 
A list was saved with the 1000 most popular keywords, and it was used in order to retrieve from it a keyword and with a GET query in the HBase, retrieve all the articles containing this keyword.

Installation & Deployment
-----------------------------------------------------------------

## Dependencies
Absolute requirement for the installation of Hadoop and HBase is that the different workers must be able to communicate with SSH without password.
So, we create once the keys in the master with the following commands:

```sh
ssh-keygen		#a pair of keys has been created
cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys
chmod 700 /root/.ssh
chmod 600 /root/id_rsa
```

Afterwards, we have to edit the /etc/hosts file of all computers so that they know each other via hostname. For example, if they all belong to the same private network (taking the first 3 IPs), insert the following lines in the /etc/hosts :
```xml
192.168.0.2 	master
192.168.0.3 	slave1
192.168.0.4 	slave2

In order to have the keys created in all PCs, we run the following commands in the slaves in order to copy them from the master :
```sh
cd /root/
scp -r .ssh slave1:/root/ 		#password will be asked
scp -r .ssh slave2:/root/ 		#password will be asked
ssh slave1 		#if everything went well, we will be able to connect without password

Another Requirement is that JVM has been installed in master and slaves

## Hadoop Installation

The following commands have to be executed in both slaves and master for the installation of Hadoop:
```sh
wget http://www.google.com/url?q=http%3A%2F%2Fapache.mirrors.tds.net%2Fhadoop%2Fcommon%2Fhadoop-1.2.1%2Fhadoop-1.2.1.tar.gz&sa=D&sntz=1&usg=AFQjCNH5Wreo7yfzUrHxqGC620b0haUZpw
cd /opt 			#if /opt/jdk1.7.0_51 is the directory containing the JVM
tar xvzf hadoop-1.2.1.tar.gz
cd hadoop-1.2.1
```

Edit the configuration files of Hadoop :
* hadoop-env.sh : set the JAVA_HOME environment variable with the path of JVM
* core-site.xml : hdfs and hadoop configurations
* hdfs-site.xml : hdfs further configurations (block size, replication etc.)
* mapred-site.xml : map reduce further configurations (java options, tracker, scheduler etc.)

Edit the hostnames in PCs with the hostname commands and then run in all the PCs the commands :
```sh
echo "master" > conf/masters
echo "slave1" > conf/slaves
echo "slave2" > conf/slaves
```

Now, we can start Hadoop with the following commands :
```sh
ssh root@master
cd /opt/hadoop-1.2.1/
bin/hadoop namenode -format			#the first time, we have to format the namenode of HDFS
bin/start-all.sh
```

## HBase Installation 
Execute the following commands in all PCs :
```sh
wget http://mirrors.gigenet.com/apache/hbase/hbase0.94.16/hbase0.94.16.tar.gz 			#download HBase
tar xvzf hbase0.94.16.tar.gz
cd hbase-0.94.16
cd /opt/hadoop1.2.1/hadoopcore1.2.1.jar/opt/hbase0.94.16/lib/
rm /opt/hbase0.94.16/lib/hadoopcore1.0.4.jar 				#connection of Hadoop with Hbase
cd /opt/hbase0.94.16/hbase0.94.16.jar/opt/hadoop1.2.1/lib/
cd /opt/hbase0.94.16/lib/zookeeper3.4.5.jar/opt/hadoop1.2.1/lib/
cd /opt/hbase0.94.16/lib/protobufjava2.4.0a.jar/opt/hadoop1.2.1/lib/
cd /opt/hbase0.94.16/lib/guava11.0.2.jar/opt/hadoop1.2.1/lib
cp /opt/hadoop1.2.1/conf/hdfssite.xml /opt/hbase0.94.16/conf/
cp /opt/hbase0.94.16/conf/hbasesite.xml /opt/hadoop1.2.1/conf/
```

Edit the configuration files of HBase (in conf folder):
* regionservers file containing dns names
* hbase-site.xml containing the configuration of HBase
* hbase-env.sh environment variables (like JVM, HBase heap size, if Hbase manages Zookeper etc.)

Start HBase :
```sh
bin/stop-hbase.sh
```

## Execution of projects-tasks 
For the projects 1-6, we simply execute the MapReduce jobs with the required argument parameters
For the projects of HBase, the following additional commands are needed

7. Wikipedia titles insertion in HBase
Execute the following commands :
```sh
hbase shell
create 'content','title'		#creation of the needed table in Hbase 
exit
hadoop jar PrepareInput.jar adts.PrepareInput <file containing wiki titles> /user/root/3.1   #create a tab-delimited file containing (MD5, titles)
hadoop jar /opt/hbase-0.94.16/hbase-0.94.16.jar importtsv -Dimporttsv.bulk.output=output -Dimporttsv.columns=HBASE_ROW_KEY,title content /user/root/3.1/part-r-00000  #creating the Region files (in the URI /user/root/output) needed for bulk loading
hadoop dfs -chown -R   hbase:hbase/user/root/output 		#change permissions so that Hbase can modify
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles output content
```

8. Inverted Index 
Execute the following commands :
```sh
hbase shell
create 'index','keyword','article'
exit
hadoop jar CreateCorrespondences.jar adts.CreateCorrespondences /user/root/3.2  #eead from 'content' table and write in region files the invert-index records
adoop jar /opt/hbase-0.94.16/hbase-0.94.16.jar importtsv -Dimporttsv.bulk.output=output -Dimporttsv.columns=HBASE_ROW_KEY,keyword,article index /user/root/3.2/part-r-00000    #creating the Region files (in the URI /user/root/output) needed for bulk loading
hadoop dfs -chown -R   hbase:hbase/user/root/3.2/part-r-00000  #changing the permission so that Hbase can modify
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles output index

