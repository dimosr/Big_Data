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


import java.io.DataInput;
import java.io.DataOutput;
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

public class Histogram {

	//mapper for the stopwords
	public static class Map1 extends Mapper<LongWritable, Text, Text, Record> {
		Text out_key = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String word = value.toString();
			String out_key_str = get_key(word);
			out_key.set(out_key_str);
			context.write(out_key,new Record(true,word));
			
		}
	}
	
	//mapper for the words of the wikipedia titles
	public static class Map2 extends Mapper<LongWritable, Text, Text, Record> {
		Text out_key = new Text();
		String word, out_key_str;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
		
			//emit a word from an article title 
			StringTokenizer tokenizer = new StringTokenizer(line,"_");
			while (tokenizer.hasMoreTokens()) {
				word = tokenizer.nextToken();
				out_key_str = get_key(word);
				out_key.set(out_key_str);
				context.write(out_key, new Record(false,word));
			}				
		}
		
		//Uncomment to manipulate the input size
		@Override
		public void run(Context context) throws IOException, InterruptedException {
			Random random = new Random();
			int iter = 0;
			int next;
			setup(context);
		    try {
		    	while ((context.nextKeyValue()) && (iter < 1000)){
		    		next = random.nextInt(10000); 
		    		for (int i = 0; (i < next) && context.nextKeyValue(); i++){
		    			context.getCurrentKey();
		    			context.getCurrentValue();
		    		}
		    		if (context.nextKeyValue())
		    			map(context.getCurrentKey(), context.getCurrentValue(), context);
		    		iter++;
		        }
		    } 
		    finally {
		    	cleanup(context);
		    }
		}
	}
	
	//mapper for the 2nd mr job that transforms numbers to percentages
	public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
		Text out_key = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			out_key.set("1");
			context.write(out_key, value);
		}
	}
	
	//reducer for the 1st mr job
	public static class Reduce1 extends Reducer<Text, Record, Text, IntWritable> {
		public void reduce(Text key, Iterable<Record> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			HashSet<String> stop_words = new HashSet<String>();
			//duplicate values list
			ArrayList<Record> dupl_values = new ArrayList<Record>();
			//create hash map that contains the stop words
			for (Record value : values){
				dupl_values.add(new Record(value.isStopWord(),value.getWord()));
				if (value.isStopWord() == true)
					stop_words.add(value.getWord());
			}
			//count words, exclude stop words
			for (Record value : dupl_values)
				if (value.isStopWord() == false)
					if (!stop_words.contains(value.getWord().toLowerCase()))
							sum++;
			context.write(key, new IntWritable(sum));
		}
	}
	
	//reducer for the 2nd mr job
	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String,Integer> hashmap = new HashMap<String,Integer>();
			int sum = 0;
			for (Text value : values){
				StringTokenizer tokenizer = new StringTokenizer(value.toString(),"\t");
				String key_str = tokenizer.nextToken();
				int count = Integer.parseInt(tokenizer.nextToken());
				sum += count;
				hashmap.put(key_str,count);
			}
			String[] firstLetters = {"A*","B*","C*","D*","E*","F*","G*","H*","I*","J*","K*","L*","M*",
								"N*","O*","P*","Q*","R*","S*","T*","U*","V*","W*","X*","Y*","Z*",
								"0...9","~!@#$%^&*()_+{}|:\"<>?[]\\;',./'"};
			for (String firstLetter : firstLetters)
				context.write(new Text(firstLetter), new Text(Float.toString(100*hashmap.get(firstLetter) / (float) sum) + " %"));
		}	
	}
	
	public static class Record implements Writable {
		private boolean is_stop_word;
		private String word;
		
		public Record(){
		}
		
		public Record(boolean is_stop_word, String word){
			this.is_stop_word = is_stop_word;
			this.word = word;
		}
		
		public boolean isStopWord(){
			return this.is_stop_word;
		}
		
		public String getWord(){
			return this.word;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeBoolean(this.is_stop_word);
			out.writeUTF(this.word);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			this.is_stop_word = in.readBoolean();
			this.word = in.readUTF();
		}
		
	}
		
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job1 = new Job(conf, "histogram_tmp");
		job1.setJarByClass(Histogram.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Record.class);
		job1.setReducerClass(Reduce1.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		Path path1 = new Path(args[0]);
	    Path path2 = new Path(args[1]);
	    MultipleInputs.addInputPath(job1, path1, TextInputFormat.class, Map1.class);
	    MultipleInputs.addInputPath(job1, path2, TextInputFormat.class, Map2.class);
		
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		
		job1.setNumReduceTasks(28);
		job1.waitForCompletion(true);
		
		Job job2 = new Job(conf,"histogram_final");
		job2.setJarByClass(Histogram.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(Map3.class);
		job2.setReducerClass(Reduce2.class);
		
		for (int i = 0; i<28; i++){
			if (i < 10)
				FileInputFormat.addInputPath(job2, new Path(args[2] + "/part-r-0000" + Integer.toString(i)));
			else
				FileInputFormat.addInputPath(job2, new Path(args[2] + "/part-r-000" + Integer.toString(i)));
		}
		FileInputFormat.addInputPath(job2, new Path(args[2] + "/part-r-00000"));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		job2.setNumReduceTasks(1);
		job2.waitForCompletion(true);
			
	} 
	
	static String get_key(String word){;
		String key;
		char first_char = word.charAt(0);
		if (((first_char >= 'a') && (first_char <= 'z')) || (first_char >= 'A') && (first_char <= 'Z'))
			key = Character.toUpperCase(first_char) + "*";
		else if ((first_char >= '0') && (first_char <= '9'))
			key = "0...9";
		else 
			key = "~!@#$%^&*()_+{}|:\"<>?[]\\;',./'";
		return key;
		
	}
}