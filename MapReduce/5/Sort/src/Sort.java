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


import java.io.IOException; 
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class Sort {

	//mapper for the stopwords
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		Text out_key = new Text();
		Text out_val = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String word = value.toString();
			out_key.set(word);
			out_val.set("stop");
			context.write(out_key,out_val);
		}
	}
	
	//mapper for the words of the wikipedia titles
	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		Text out_key = new Text();
		Text out_val = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String word;
			//emit a word from an article title 
			StringTokenizer tokenizer = new StringTokenizer(line,"_");
			while (tokenizer.hasMoreTokens()) {
				word = tokenizer.nextToken();
				out_key.set(word.toLowerCase());
				out_val.set("nostop");
				context.write(out_key, out_val);
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			boolean flag = false;
			for (Text value : values){
				if (value.toString().equals("stop"))
					flag = true;
			}
			if (flag == false)
				context.write(key, new Text(""));
		}
	}
		
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		TotalOrderPartitioner.setPartitionFile(conf, new Path(args[2]));
		Job job = new Job(conf, "sort");
		job.setJarByClass(Sort.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path path1 = new Path(args[0]);
	    Path path2 = new Path(args[1]);
	    MultipleInputs.addInputPath(job, path1, TextInputFormat.class, Map1.class);
	    MultipleInputs.addInputPath(job, path2, TextInputFormat.class, Map2.class);
		
		job.setReducerClass(Reduce.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		job.setPartitionerClass(TotalOrderPartitioner.class);
		job.setNumReduceTasks(10);
		job.waitForCompletion(true);
	} 
	
}