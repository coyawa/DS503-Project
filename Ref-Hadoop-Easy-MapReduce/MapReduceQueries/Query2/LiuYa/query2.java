import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
	
public class query2{
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{
		private final static IntWritable custID = new IntWritable(0);
		private Text transTotal = new Text();

		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter)throws IOException{
			String line = value.toString();
			String[] splits = line.split(",");
			custID.set(Integer.parseInt(splits[1]));
			transTotal.set("1,"+splits[2]);
			output.collect(custID,transTotal);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>{
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter)throws IOException{
			long num = 0;
			double sum = 0;
			while (values.hasNext()){
				num +=1;
			    String total = values.next().toString().split(",")[1];
				sum +=Float.parseFloat(total);
			}
			Text text = new Text();
			text.set(Long.toString(num)+","+String.valueOf(sum));
			output.collect(key,text);	
		}
	}
	public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(query2.class);
      conf.setJobName("query2");

      conf.setOutputKeyClass(IntWritable.class);
      conf.setOutputValueClass(Text.class);

      conf.setMapperClass(Map.class);
      //conf.setCombinerClass(Reduce.class);
      conf.setReducerClass(Reduce.class);

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
	
      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));

      JobClient.runJob(conf);
    }
}
