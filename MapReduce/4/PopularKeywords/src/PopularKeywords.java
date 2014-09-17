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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import adts.SuccessfullQueries.Map;
import adts.SuccessfullQueries.Reduce;

public class PopularKeywords {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();

	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line,"\t");
	        tokenizer.nextToken();
	        String second_column = tokenizer.nextToken();
	        String[] keywords = second_column.split(" ");
	        for(int i = 0; i < keywords.length; i++){
	        	word.set(keywords[i]);
	        	context.write(word, one);
	        }
	        
	    }
	 }
	
	public static class StopwordsMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable two = new IntWritable(2);
	    private Text word = new Text();

	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        word.set(line);
	        context.write(word, two);
	        
	    }
	 }
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	      throws IOException, InterruptedException {
	        int sum = 0;
	        int belongs_to_stopwords = 0;
	        for (IntWritable val : values) {
	        	if( val.get() == 1 ){
	        		sum += val.get();
	        	}
	        	else if( val.get() == 2 ){
	        		belongs_to_stopwords = 1;
	        	}
	        }
	        if( belongs_to_stopwords == 0 ){
	        	context.write(key, new IntWritable(sum));
	        }
	    }
	 }
	
	public static class ReverseMap extends Mapper<LongWritable, Text, LongWritable, Text> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();

	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line,"\t");
	        String first_column = tokenizer.nextToken();
	        String second_column = tokenizer.nextToken();
	        context.write(new LongWritable(Integer.parseInt(second_column)), new Text(first_column) );
	    }
	}

	 public static class ReverseReduce extends Reducer<LongWritable, Text, Text, LongWritable> {
		private static int counter = 0;
	    public void reduce(LongWritable key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {
	        	for (Text val : values) {
	        		if( counter < 50){
	        			context.write(val, key);
	        			counter++;
	        		}
	        	}
	    }
	 
}

	
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();

	    Job job = new Job(conf, "PopularKeywords");
	    job.setJarByClass(PopularKeywords.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    job.setReducerClass(Reduce.class);

	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    Path queriesInputPath = new Path(args[0]);
	    Path StopWordsInputPath = new Path(args[1]);
	    MultipleInputs.addInputPath(job, queriesInputPath, TextInputFormat.class, Map.class);
	    MultipleInputs.addInputPath(job, StopWordsInputPath, TextInputFormat.class, StopwordsMap.class);

	    FileOutputFormat.setOutputPath(job, new Path("/root/temporary"));

	    job.waitForCompletion(true);
	    
	    Job sortingJob = new Job(conf, "PopularKeywords");
	    sortingJob.setJarByClass(PopularKeywords.class);
	    
	    sortingJob.setOutputKeyClass(Text.class);
	    sortingJob.setOutputValueClass(LongWritable.class);
	    
	    sortingJob.setMapperClass(ReverseMap.class);
	    sortingJob.setReducerClass(ReverseReduce.class);
	    
	    sortingJob.setInputFormatClass(TextInputFormat.class);
	    sortingJob.setOutputFormatClass(TextOutputFormat.class);
	    sortingJob.setSortComparatorClass(LongWritable.DecreasingComparator.class);
	    sortingJob.setMapOutputKeyClass(LongWritable.class);
	    sortingJob.setMapOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(sortingJob, new Path("/root/temporary"));
	    FileOutputFormat.setOutputPath(sortingJob, new Path(args[2]));
	    
	    sortingJob.setNumReduceTasks(1);
	    sortingJob.waitForCompletion(true);
	 }
	
}

