import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class query3{
	
	public static class CustomerMap extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{
        //variables to process Customer details		
		private  IntWritable custID = new IntWritable(0);
		private String name, salary, fileTag ="C,";
		
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter)throws IOException{
			String line = value.toString();
			String[] splits = line.split(",");
			custID.set(Integer.parseInt(splits[0]));
			name = splits[1];
			salary = splits[4];
			output.collect(custID,new Text(fileTag + name + "," + salary));
		}
	}
	
	public static class TransactionMap extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{
		//variables to process Transaction details	
		private IntWritable custID = new IntWritable(0);
		private String transTotal,transNumItems,fileTag ="T,";
		
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter)throws IOException{
			String line = value.toString();
			String[] splits = line.split(",");
			custID.set(Integer.parseInt(splits[1]));
			transTotal = splits[2];
			transNumItems = splits[3];
			output.collect(custID,new Text(fileTag + transTotal + "," + transNumItems));
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>{
		//variebles to aid the join process
		private String name, salary, transTotal, transNumItems, minItem;
		private long num;    //the number of transactions done by the customer
		private double sum;  //the sum of the field "TransTotal" for that customer
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter)throws IOException{
			minItem = "10";
			num = 0;
			sum = 0;
			while (values.hasNext()){
				String line = values.next().toString();
				String splits[] = line.split(",");
			
				if(splits[0].equals("C")){
					name = splits[1];
					salary = splits[2];
				}
				else if(splits[0].equals("T")){
					transTotal = splits[1];
					transNumItems = splits[2];
					num += 1;
					sum +=Float.parseFloat(transTotal);
					if(Integer.parseInt(minItem) < Integer.parseInt(transNumItems)){
						minItem = transNumItems;
					}
				}
			}
			Text text = new Text();
			text.set(name+"," + salary + "," + Long.toString(num) + "," + String.valueOf(sum) + "," + minItem);
			output.collect(key,text);	
		}
	}

	public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(query3.class);
      conf.setJobName("query3");

      conf.setOutputKeyClass(IntWritable.class);
      conf.setOutputValueClass(Text.class);

      //conf.setMapperClass(CustomerMap.class);
	  //conf.setMapperClass(TransactionMap.class);
      //conf.setCombinerClass(Reduce.class);
      conf.setReducerClass(Reduce.class);

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
	
      MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, CustomerMap.class);
      MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, TransactionMap.class);
      FileOutputFormat.setOutputPath(conf, new Path(args[2]));

      JobClient.runJob(conf);
    }
}
