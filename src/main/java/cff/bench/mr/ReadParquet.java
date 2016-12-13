package cff.bench.mr;

import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.parquet.hadoop.ParquetInputFormat;
//import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
//import static org.apache.parquet.filter2.predicate.FilterApi.and;
//import static org.apache.parquet.filter2.predicate.FilterApi.eq;
//import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
//import static org.apache.parquet.filter2.predicate.FilterApi.not;
//import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
//import static org.apache.parquet.filter2.predicate.FilterApi.or;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class ReadParquet extends Configured implements Tool {

	public class Commands {
		@Parameter(names = { "-in" }, description = "Input file or directory")
		private String in;
		@Parameter(names = { "-out" }, description = "Output directory")
		private String out;
		@Parameter(names = { "-cols" }, description = "Columns to read. Ex. 'ColA,ColB,ColC' for columns ColA, ColB, and Colc")
		private List<String> cols;
	}

	public int run(String[] args) throws Exception {
		Commands commands = new Commands();
		new JCommander(commands, args);

		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}

//		for(String col : commands.cols){
//			intColumn(col);			
//		}
//		OrcInputFormat.setSearchArgument(conf, sarg, cols.);
		
		Job job = Job.getInstance(getConf());
		job.setJobName("cff-read-parquet");
		job.setJarByClass(ReadParquet.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(CountRowsMapper.class);

		job.setInputFormatClass(ParquetInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(0);

		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);

		FileInputFormat.setInputDirRecursive(job, true);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ReadParquet convert = new ReadParquet();
		int res = ToolRunner.run(convert, args);
		System.exit(res);
	}
}