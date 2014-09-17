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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Partitioner {
	
	public static class Keyout implements WritableComparable<Keyout> {
		
		private String type;
		private String word;
		
		public Keyout(){
		}
		
		public Keyout(String word){
			if (word.equals(""))
				this.type = "count";
			else
				this.type = "word";
			this.word = word.toLowerCase();
		}
		
		public String getType(){
			return this.type;
		}
		
		public String getWord(){
			return this.word;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(this.type);
			out.writeUTF(this.word);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			this.type = in.readUTF();
			this.word = in.readUTF();
		}
		
		@Override
		public int compareTo(Keyout key2) {
			if ((this.getType().equals("count")) && (key2.getType().equals("word")))
				return -1;
			else if ((this.getType().equals("word")) && (key2.getType().equals("count")))
				return 1;
			else 
				return this.word.compareTo(key2.getWord());
			
		}
		
	}

	public static class Map extends Mapper<LongWritable, Text, Keyout, Text> {
		Keyout out_key;
		Text out_val;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String word;
			//emit a word from an article title 
			StringTokenizer tokenizer = new StringTokenizer(line,"_");
			out_key = new Keyout("");
			out_val = new Text(Integer.toString(tokenizer.countTokens()));
			context.write(out_key,out_val);
			while (tokenizer.hasMoreTokens()) {
				word = tokenizer.nextToken();
				out_key = new Keyout(word);
				out_val = new Text("");
				context.write(out_key, out_val);
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
		    	while ((context.nextKeyValue()) && (iter < 50)){
		    		next = random.nextInt(200000); 
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
	
	public static class Reduce extends Reducer<Keyout, Text, Text, NullWritable> {
		int total = 0;
		int count = 0;
		public void reduce(Keyout key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (key.getType().equals("count")){
				for (Text value : values)
					total += Integer.parseInt(value.toString());
			}
			else if (key.getType().equals("word")){
				for (Text value : values){
					count++;
					if (count > total / 10){
						context.write(new Text(key.getWord()), NullWritable.get());
						count = 	0;
					}	
				}
			}
		}
	}
	
		
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "partitioner50");
		job.setJarByClass(Partitioner.class);
		job.setMapOutputKeyClass(Keyout.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	} 
}