import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
	
public class transforCust{
public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, FloatWritable> {
      private IntWritable custID=new IntWritable(0);
      //private Text stringStoreTwoInts = new Text();
      private FloatWritable transTotal=new FloatWritable(0);

      public void map(LongWritable key, Text value, OutputCollector<IntWritable, FloatWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] splits = line.split(",");
        custID.set(Integer.parseInt(splits[1]));
	//stringStoreTwoInts="1,"+splits[2];
	transTotal.set(Float.parseFloat(splits[2]));
        output.collect(custID, transTotal);     
      }
    }

    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, FloatWritable, IntWritable, Text> {
      public void reduce( IntWritable key, Iterator<FloatWritable> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {      
	long numTrans = 0;
        double totalSum=0;
        while (values.hasNext()) {	  
          totalSum += Float.parseFloat(values.next().toString());
	  numTrans++;
        }
	String stringStoreTwoInts=Long.toString(numTrans)+Double.toString(totalSum);
        output.collect(key, new Text(stringStoreTwoInts));
      }
    }
    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(transforCust.class);
      conf.setJobName("transforcust");

      conf.setOutputKeyClass(IntWritable.class);
      conf.setOutputValueClass(FloatWritable.class);

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
		   


