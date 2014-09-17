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
import java.security.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;


public class CreateCorrespondences {
	
	public static class Map extends TableMapper<Text, Text>  {
		public static final byte[] TITLE = "title".getBytes();
		public static final byte[] ATTR1 = "".getBytes();

	   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
	        	String val = new String(value.getValue(TITLE, ATTR1));
	        	String row_key = new String (value.getRow());
	  
	        	String[] keywords = val.split("_");
	        	for(int i = 0; i < keywords.length; i++){
	        		context.write(new Text(keywords[i]), new Text(row_key));
	        	}    	
	   	}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>  {
		MessageDigest md;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String keyword = new String(key.toString());
			String md5,value ;
			
			try {
					md = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
			}
			for (Text val : values) {
				md5 = new String(val.toString());
				value = new String(keyword + "," + md5);
				md.update(value.getBytes());
				byte[] thedigest = md.digest();
		        StringBuffer sb = new StringBuffer();
		        for (int i = 0; i < thedigest.length; i++)
		            sb.append(Integer.toString((thedigest[i] & 0xff) + 0x100, 16).substring(1));
		        String keyword_and_article_md5 = key.toString() + "\t" + val.toString();
				context.write(new Text(sb.toString()), new Text(keyword_and_article_md5) );
			}
			
		}
	}
	 
	public static void main(String[] args) throws Exception {
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "CreateCorrespondences");
		job.setJarByClass(CreateCorrespondences.class);     
		
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob(
		  "content",        // input HBase table name
		  scan,             // Scan instance to control CF and attribute selection
		  Map.class,   // mapper
		  Text.class,             // mapper output key
		  Text.class,             // mapper output value
		  job);
		job.setReducerClass(Reduce.class);    
		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		boolean b = job.waitForCompletion(true);
		if (!b) {
		  throw new IOException("error with job!");
		}
	 }

}