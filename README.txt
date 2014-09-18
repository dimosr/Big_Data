

		---------------------------------------------------------
		|							|
		|		Big Data - Batch Processing - Hadoop	|
		|							|
		---------------------------------------------------------


	This repository contains implementations around Big Data and Batch Processing with use of 
	Hadoop, MapReduce algorithm and HBase.

	MapReduce was used to save and analyze Big Data sets.
	HBase was used to insert and edit the prrevious data in order to be able to execute queries.
	The data set that was used contained real user queries in Google Search Engine and a set
	containing the titles of all Wikipedia articles.

	The file containing the queries was taken from the dataset released from AOL
	Source : http://en.wikipedia.org/wiki/AOL_search_data_leak
	The file containing the titles of all Wikipedia articles was taken from Wikipedia 
	Source : dumps.wikimedia.org/enwiki/latest/enwiki-latest-all-titles-in-ns0.gz

	There are multiple parts/sub-projects in this repository :
	1. Searches Sum Counting Per Day
		A MapReduce algorithm was implemented in order to output dates with the number of searches
		executed during this date. This was later used to produce a graph.

	2. Successfull and Unsuccessfull Search Sum 
		A MapReduce algorithm was implemented in order to output the percentage of successfull and
		unsuccessfull queries. Successfull queries were those that resulted in a page and unsuccessfull 
		were those that did not give a page as result

	3. "Famous" Webpages
		A MapReduce algorithm was implemented in order to output a list, where the first column contained
		the URLs of webpages that were visited by more than 10 users and the second column contained those
		users. 

	4. "Popular" Keywords in Searches
		A MapReduce algorithm was implemented in order to output a list, where the first column contained
		the 50 most searched keywords and the second column contained the number each keyword was used.
		The "noise" was removed by removing the unnecessary stop words, by using the list from
		http://jmlr.csail.mit.edu/papers/volume5/lewis04a/a11-smart-stop-list/english.stop

	5. Histograms of Lexicographic Distribution.
		A MapReduce algorithm was implemented in order to create histogram of different categories of keywords
		depending on the starting letter. The Histogram program was used to create 3 histograms with different 
		sample size : 50,1000 and all the Wikipedia titles. The Partitioner and Sorter programs were used to 
		partition in almost same-sized categories the keywords and afterwards sort them alphabetically.

	6. Wikipedia - resolvable queries
		A MapReduce algorithm was implemented in order to calculate the percentage of queries that can be 
		answered by Wikipedia. Those queries must contain at least a keyword that is contained in a Wikipedia
		article title.

	7. Wikipedia titles insertion in HBase
		A MapReduce algorithm was implemented in order to insert all the Wikipedia titles in the HBase.
		For each title, an identifier was created by encrypting with MD-5 the title string.
		The table 'content' that was created contained the identifier and the title of the article.
	8. Inverted Index 
		A MapReduce algorithm was implemented in order to create an inverted index of records like
		keyword : {set of articles containing this keyword}
		A new table 'index' was created from this MapReduce in the HBase, that contained the inverted
		index.

	9. Queries Execution in HBase
		A MapReduce algorithm was implemented in order to make random queries in the Hbase.
		A list was saved with the 1000 most popular keywords, and it was used in order to 
		retrieve from it a keyword and with a GET query in the HBase, retrieve all the articles
		containing this keyword.
