package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> <input dir> <output dir>");
	    System.exit(-1);
	} else if ("RespCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(RespCount.ReducerImpl.class);
	    job.setMapperClass(RespCount.MapperImpl.class);
	    job.setOutputKeyClass(RespCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(RespCount.OUTPUT_VALUE_CLASS);
	} else if ("ReqCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(ReqCount.ReducerImpl.class);
	    job.setMapperClass(ReqCount.MapperImpl.class);
	    job.setOutputKeyClass(ReqCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(ReqCount.OUTPUT_VALUE_CLASS);
	} else if ("ByteCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(ByteCount.ReducerImpl.class);
	    job.setMapperClass(ByteCount.MapperImpl.class);
	    job.setOutputKeyClass(ByteCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(ByteCount.OUTPUT_VALUE_CLASS);
	} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog.ReducerImpl.class);
	    job.setMapperClass(AccessLog.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
	} else if ("DateCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(DateCount.ReducerImpl.class);
	    job.setMapperClass(DateCount.MapperImpl.class);
	    job.setOutputKeyClass(DateCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(DateCount.OUTPUT_VALUE_CLASS);
	} else if ("DateByteCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(DateByteCount.ReducerImpl.class);
	    job.setMapperClass(DateByteCount.MapperImpl.class);
	    job.setOutputKeyClass(DateByteCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(DateByteCount.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path("tempout"));
		Job j2 = new Job(conf, "sorting");
		j2.setMapperClass(Sorter.MapperImpl.class);
		j2.setReducerClass(Sorter.ReducerImpl.class);
		j2.setOutputKeyClass(Sorter.OUTPUT_KEY_CLASS);
		j2.setOutputValueClass(Sorter.OUTPUT_VALUE_CLASS);
		job.waitForCompletion(true);
		FileInputFormat.addInputPath(j2, new Path("tempout"));
		FileOutputFormat.setOutputPath(j2, new Path(otherArgs[2]));
		System.exit(j2.waitForCompletion(true) ? 0 : 1);
	} else if ("HostReqCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(HostReqCount.ReducerImpl.class);
	    job.setMapperClass(HostReqCount.MapperImpl.class);
	    job.setOutputKeyClass(HostReqCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(HostReqCount.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path("tempout"));
		Job j2 = new Job(conf, "sorting");
		j2.setMapperClass(Sorter.MapperImpl.class);
		j2.setReducerClass(Sorter.ReducerImpl.class);
		j2.setOutputKeyClass(Sorter.OUTPUT_KEY_CLASS);
		j2.setOutputValueClass(Sorter.OUTPUT_VALUE_CLASS);
		job.waitForCompletion(true);
		FileInputFormat.addInputPath(j2, new Path("tempout"));
		FileOutputFormat.setOutputPath(j2, new Path(otherArgs[2]));
		System.exit(j2.waitForCompletion(true) ? 0 : 1);
	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
