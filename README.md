Big Data - Batch Processing <br/> (with Hadoop-HBase)
===============================================

Introduction
----------------------------------------------
This repository contains implementations around Big Data and Batch Processing with use of Hadoop, MapReduce algorithm and HBase.

MapReduce was used to save and analyze Big Data sets.
HBase was used to insert and edit the prrevious data in order to be able to execute queries.
The data set that was used contained real user queries in Google Search Engine and a set containing the titles of all Wikipedia articles.

The file containing the queries was taken from the dataset released from [AOLSource](http://en.wikipedia.org/wiki/AOL_search_data_leak)
The file containing the titles of all Wikipedia articles was taken from [Wikipedia Source](dumps.wikimedia.org/enwiki/latest/enwiki-latest-all-titles-in-ns0.gz)

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
A MapReduce algorithm was implemented in order to analyse the most used keywords in queries. The output was a list, where the first column contained the 50 most searched keywords and the second column contained the number each keyword was used. The "noise" was removed by removing the unnecessary stop words, by using the following [list](http://jmlr.csail.mit.edu/papers/volume5/lewis04a/a11-smart-stop-list/english.stop)

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
