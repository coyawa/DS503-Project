import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
	
public class transforCust{
public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
      private IntWritable custID=new IntWritable(0);
      
      private Text transTotal=new Text();

      public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] splits = line.split(",");
        int custIDint=Integer.parseInt(splits[1]);
	float transTotalFloat=Float.parseFloat(splits[2]);
	custID.set(custIDint);
	transTotal.set("1,"+String.valueOf(transTotalFloat));
        output.collect(custID, transTotal);     
      }
    }

    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
      private Text stringStoreTwoInts = new Text();
      public void reduce( IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {      
	long numTrans = 0;
        double totalSum=0;
	String[] splits;
        while (values.hasNext()) {
	  splits = values.next().toString().split(",");
          totalSum += Float.parseFloat(splits[1]);
	  numTrans=numTrans+Integer.parseInt(splits[0]);
        }
	String strings=Long.toString(numTrans)+","+String.valueOf(totalSum);
	stringStoreTwoInts.set(strings);
        output.collect(key, stringStoreTwoInts);
      }
    }

    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(transforCust.class);
      conf.setJobName("transforcust");

      conf.setOutputKeyClass(IntWritable.class);
      conf.setOutputValueClass(Text.class);

      conf.setMapperClass(Map.class);
      conf.setCombinerClass(Reduce.class);
      conf.setReducerClass(Reduce.class);

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
	
      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));

      JobClient.runJob(conf);
    }
}
		   


