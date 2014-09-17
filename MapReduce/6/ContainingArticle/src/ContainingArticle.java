    /**************************************************
    Copyright (C) 2014  Raptis Dimos <raptis.dimos@yahoo.gr>


    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
	**************************************************/


package adts;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;





public class ContainingArticle {
	public static class QueriesMap extends Mapper<LongWritable, Text, Text, Text> {
	    private Text word = new Text();

	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line,"\t");
	        tokenizer.nextToken();
	        String second_column = tokenizer.nextToken();
		    String[] keywords = second_column.split(" ");
		    for(int i = 0; i < keywords.length; i++){
		        	word.set(keywords[i].toLowerCase());
		        	context.write(word, new Text(line));
		    }
	        
	    }
	 }
	
	public static class ArticlesMap extends Mapper<LongWritable, Text, Text, Text> {
	    private Text word = new Text();

	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line,"\t");
	        String first_column = tokenizer.nextToken();
        	String[] keywords = first_column.split("_");
        	for(int i = 0; i < keywords.length; i++){
	        	word.set(keywords[i].toLowerCase());
	        	context.write(word, new Text(line));
        	}
	        
	    }
	 }

	 public static class Reduce extends Reducer<Text, Text, IntWritable, Text> {

	    public void reduce(Text key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {
	    	HashSet<String> queries_and_articles = new HashSet<String>();
	    	String contains_article = new String("article");
	    	List<String> cache = new ArrayList<String>();
	        for (Text val : values) {
	        	cache.add(new String(val.toString()));
	        	String line = val.toString();
	        	StringTokenizer tokenizer = new StringTokenizer(line,"\t");
	        	if( tokenizer.countTokens() > 2 ){
	        		queries_and_articles.add( line );
	 	        }
	 	        else if( tokenizer.countTokens() < 2 ){
	 	        	queries_and_articles.add( contains_article );
	 	        }
	       }
	       if( queries_and_articles.contains( contains_article ) ){
	    	   for(String val : cache) {
		        	StringTokenizer tokenizer = new StringTokenizer(val,"\t");
		        	if( tokenizer.countTokens() > 2 ){
		        		context.write(new IntWritable(1), new Text(val) );
		 	        }
	    	   }
	       }
	       else{
	    	   for(String val : cache) {
		        	StringTokenizer tokenizer = new StringTokenizer(val,"\t");
		        	if( tokenizer.countTokens() > 2 ){
		        		context.write(new IntWritable(0), new Text(val));
		 	        }
	    	   }
	       }
	    }
	 }
	 
	 public static class CollectMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		    private final static IntWritable one = new IntWritable(1);
		    private final static IntWritable zero = new IntWritable(0);
		    private Text word = new Text();

		    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		        String line = value.toString();
		        String[] array = line.split("\t");
		        String query = "";
		        for(int i = 1; i < array.length; i++){
		        	query = query + array[i] ;
		        	if(i != array.length){
		        		query = query + "\t";
		        	}
		        }
		        if( array[0].equals("1") ){
		        	word.set(new String(query) );
		        	context.write(word, one);
		        }
		        else if(array[0].equals("0") ){
		        	word.set(new String(query) );
		        	context.write(word, zero);
		        }
		    }
		 } 

		 public static class CollectReduce extends Reducer<Text, IntWritable, IntWritable, Text> {

		    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		      throws IOException, InterruptedException {
		        int flag = 0;
		        for (IntWritable val : values) {
		        	if( val.get() == 1 )
		        		flag = 1;
		        }
		        context.write(new IntWritable(flag), key);
		    }
		 }
		 
		 public static class CountMap extends Mapper<LongWritable, Text, IntWritable, Text> {
			    private final static IntWritable one = new IntWritable(1);
			    private final static IntWritable zero = new IntWritable(0);
			    private Text word = new Text();

			    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			        String line = value.toString();
			        String[] array = line.split("\t");
			        String query = "";
			        for(int i = 1; i < array.length; i++){
			        	query = query + array[i] ;
			        	if(i != array.length){
			        		query = query + "\t";
			        	}
			        }
			        if( array[0].equals("1") ){
			        	word.set(new String(query) );
			        	context.write(one, word);
			        }
			        else if(array[0].equals("0") ){
			        	word.set(new String(query) );
			        	context.write(zero, word);
			        }
			    }
			 } 

			 public static class CountReduce extends Reducer<IntWritable, Text, Text, IntWritable> {

			    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
			      throws IOException, InterruptedException {
			    	int sum = 0;
			    	for (Text val : values) {
		    			sum++;
		    		}
			    	if( key.get() == 1){
			    		context.write(new Text("Q_exist"), new IntWritable(sum));
			    	}
			    	else if( key.get() == 0){
			    		context.write(new Text("Q_not_exist"), new IntWritable(sum));
			    	}
			    }
			 }

	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();

	    Job job = new Job(conf, "ContainingArticle");
	    job.setJarByClass(ContainingArticle.class);

	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);

	    job.setReducerClass(Reduce.class);

	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    Path queriesInputPath = new Path(args[0]);
	    Path articlesInputPath = new Path(args[1]);
	    MultipleInputs.addInputPath(job, queriesInputPath, TextInputFormat.class, QueriesMap.class);
	    MultipleInputs.addInputPath(job, articlesInputPath, TextInputFormat.class, ArticlesMap.class);
	    
	    FileOutputFormat.setOutputPath(job, new Path("/root/temporary"));
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);

	    job.waitForCompletion(true);
	    
	    Job collectingJob = new Job(conf, "ContainingArticle");
	    collectingJob.setJarByClass(ContainingArticle.class);
	    
	    collectingJob.setOutputKeyClass(IntWritable.class);
	    collectingJob.setOutputValueClass(Text.class);
	    
	    collectingJob.setMapperClass(CollectMap.class);
	    collectingJob.setReducerClass(CollectReduce.class);
	    
	    collectingJob.setInputFormatClass(TextInputFormat.class);
	    collectingJob.setOutputFormatClass(TextOutputFormat.class);
	    collectingJob.setMapOutputKeyClass(Text.class);
	    collectingJob.setMapOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(collectingJob, new Path("/root/temporary"));
	    FileOutputFormat.setOutputPath(collectingJob, new Path("/root/temporary2"));

	    collectingJob.waitForCompletion(true);
	    
	    Job countingJob = new Job(conf, "ContainingArticle");
	    countingJob.setJarByClass(ContainingArticle.class);
	    
	    countingJob.setOutputKeyClass(Text.class);
	    countingJob.setOutputValueClass(IntWritable.class);
	    
	    countingJob.setMapperClass(CountMap.class);
	    countingJob.setReducerClass(CountReduce.class);
	    
	    countingJob.setInputFormatClass(TextInputFormat.class);
	    countingJob.setOutputFormatClass(TextOutputFormat.class);
	    countingJob.setMapOutputKeyClass(IntWritable.class);
	    countingJob.setMapOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(countingJob, new Path("/root/temporary2"));
	    FileOutputFormat.setOutputPath(countingJob, new Path(args[2]));

	    countingJob.waitForCompletion(true);
	 }
	 
}
