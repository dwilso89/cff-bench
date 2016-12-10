package cff.bench.convert;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.hadoop.ParquetOutputFormat;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class JobRunnerSomething extends Configured implements Tool {

	public class Commands {
		@Parameter
		private List<String> parameters = new ArrayList<>();
	}

	public int run(String[] args) throws Exception {
		Commands commands = new Commands();
		new JCommander(commands, args);

		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}

		Job job = Job.getInstance(getConf());
		job.setJobName("cff-2parquet");
		job.setJarByClass(JobRunnerSomething.class);

		job.setOutputKeyClass(Void.class);
		job.setOutputValueClass(IntWritable.class);

//		job.setMapperClass(CsvToParquetMapper.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(ParquetOutputFormat.class);

		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);

		FileInputFormat.setInputDirRecursive(job, true);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		JobRunnerSomething convert = new JobRunnerSomething();
		int res = ToolRunner.run(convert, args);
		System.exit(res);
	}
}