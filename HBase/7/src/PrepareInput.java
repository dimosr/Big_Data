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
import java.security.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import adts.SuccessfullQueries.Map;
import adts.SuccessfullQueries.Reduce;


public class PrepareInput {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	    MessageDigest md;

	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        try {
				md = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        md.update(line.getBytes());
	        byte[] thedigest = md.digest();
	        StringBuffer sb = new StringBuffer();
	        for (int i = 0; i < thedigest.length; i++)
	            sb.append(Integer.toString((thedigest[i] & 0xff) + 0x100, 16).substring(1));
	        context.write(new Text(sb.toString()), new Text(line));
	    }
	 } 

	 public static class Reduce extends Reducer<Text, Text, Text, Text> {

	    public void reduce(Text key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {
	        for (Text val : values) {
	        	context.write(key, val);
	        }
	       
	    }
	 }
	 
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();

		    Job job = new Job(conf, "PrepareInput");
		    job.setJarByClass(PrepareInput.class);

		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);

		    job.setMapperClass(Map.class);
		    job.setReducerClass(Reduce.class);

		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);

		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		    job.setNumReduceTasks(1);
		    job.waitForCompletion(true);
	 }

}
