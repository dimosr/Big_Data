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

import java.io.BufferedReader;
import java.io.IOException;


import java.io.InputStreamReader;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.rest.protobuf.generated.CellMessage.Cell;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseClient {
	//reads from the file with the most popular keywords(args[0]) and produces the suitable queries
	public static void main(String[] args) throws IOException {
		String[] keys = new String[5];
		int keywords_counter = 0;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path inFile = new Path(args[0]);
		if (!fs.exists(inFile))
			  System.out.println("Input file not found");
		if (!fs.isFile(inFile))
			System.out.println("Input should be a file");
		else{
			FSDataInputStream fsDataInputStream = fs.open(inFile);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
			String line;
			while ( ( (line= bufferedReader.readLine())!=null ) && (keywords_counter < 5) ) {
					String[] array = line.split("\t");
					String keyword = array[0];
					System.out.println("Record :	" + keyword);
					keys[keywords_counter] = keyword;
					keywords_counter++;
			}
			bufferedReader.close();
			fs.close();
			
			
			Configuration config = HBaseConfiguration.create();
			HTable table = new HTable(config, "index");
			
			Random randomGenerator = new Random();
			for(int i = 0; i < 10; i++){
				int randomInt = randomGenerator.nextInt(5);
				System.out.println("Random chosen keyword : " + keys[randomInt]);
				
				FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
				SingleColumnValueFilter filter_by_name = new SingleColumnValueFilter( 
		                   Bytes.toBytes("keyword"),
		                   Bytes.toBytes(""),
		                   CompareOp.EQUAL,
		                   Bytes.toBytes(keys[randomInt]));
				//filter_by_name.setFilterIfMissing(true);
				list.addFilter(filter_by_name);
				
				Scan scan = new Scan();
				scan.setFilter(list);
				//scan.addFamily(Bytes.toBytes("keyword"));
				ResultScanner scanner = table.getScanner(scan);
				try {

				      for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
				        // print out the row we found and the columns we were looking for
				    	byte[] cells = rr.getValue(Bytes.toBytes("article"), Bytes.toBytes(""));
				        System.out.println("Keyword " + keys[randomInt] + "belonging to article with md5 : "  + Bytes.toString(cells));
				      }
				} 
				catch(Exception e){
					e.printStackTrace();
				}
				finally {
				      scanner.close();
				}
					
			}
			table.close();
			
		}
		
	}

}
