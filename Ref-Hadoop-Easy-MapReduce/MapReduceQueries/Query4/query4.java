import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class query4{
	public static class CustomerMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
        //variables to process Customer details		
		private String custID, countryCode, fileTag ="C,";
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
			String line = value.toString();
			String[] splits = line.split(",");
			custID = splits[0];
			countryCode = splits[3];
			output.collect(new Text(custID),new Text(fileTag + countryCode));
		}
	}
	
	public static class TransactionMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
		//variables to process Transaction details	
		private String custID, transTotal,fileTag ="T,";
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
			String line = value.toString();
			String[] splits = line.split(",");
			custID = splits[1];
			transTotal = splits[2];
			output.collect(new Text(custID),new Text(fileTag + transTotal));
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
		//variebles to aid the join process
		private String countryCode, transTotal, min_transTotal, max_transTotal;
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
			min_transTotal = "1000";
			max_transTotal = "10";
			while (values.hasNext()){
				String line = values.next().toString();
				String splits[] = line.split(",");
			
				if(splits[0].equals("C")){
					countryCode = splits[1];
				}
				else if(splits[0].equals("T")){
					transTotal = splits[1];
					if(Float.parseFloat(transTotal) < Float.parseFloat(min_transTotal)){min_transTotal = transTotal;}
					if(Float.parseFloat(transTotal) > Float.parseFloat(max_transTotal)){max_transTotal = transTotal;}
				}
			}
			Text text = new Text(countryCode + "," + min_transTotal + "," + max_transTotal);
			output.collect(new Text(),text);	
		}
	}

	public static class JoinMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

		//variables to process Customer details		
		private  String countryCode;
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{

			String line = value.toString();
			String[] splits = line.split(",");
			countryCode = splits[0].trim();
			output.collect(new Text(countryCode),new Text(line));
		}
	}
	
	public static class JoinReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
		//variebles to aid the join process
		private String countryCode, custNo, min_transTotal_ofAll, max_transTotal_ofAll;
		private long num;
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
			min_transTotal_ofAll = "1000";
			max_transTotal_ofAll= "10";
			String min_transTotal, max_transTotal;
			num = 0;
			while (values.hasNext()){
				num += 1;
				String line = values.next().toString();
				String splits[] = line.split(",");
				min_transTotal = splits[1];
				max_transTotal = splits[2];
				if(Float.parseFloat(min_transTotal) < Float.parseFloat(min_transTotal_ofAll))
					 min_transTotal_ofAll = min_transTotal;
				if(Float.parseFloat(max_transTotal) > Float.parseFloat(max_transTotal_ofAll))
					 max_transTotal_ofAll = max_transTotal;
			}
			Text text = new Text();
			custNo = Long.toString(num);
			text.set(custNo + "," + min_transTotal_ofAll + "," + max_transTotal_ofAll);
			output.collect(key,text);	
		}
	}
	public static void main(String[] args) throws Exception {
        //JOB1
		JobConf conf = new JobConf(query4.class);
        conf.setJobName("query4-1");

        conf.setOutputKeyClass(Text.class);
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

		//JOB2
	    JobConf conf1 = new JobConf(query4.class);
        conf1.setJobName("query4-2");

        conf1.setOutputKeyClass(Text.class);
        conf1.setOutputValueClass(Text.class);
		conf1.setMapperClass(JoinMap.class);
		conf1.setReducerClass(JoinReduce.class);
        conf1.setInputFormat(TextInputFormat.class);
        conf1.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf1, new Path(args[2]));
	    FileOutputFormat.setOutputPath(conf1, new Path(args[3]));
		JobClient.runJob(conf1);

    }
}
