package cff.bench.mr;

import java.io.File;
import java.io.FileInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.OrcConf;
import org.apache.orc.mapreduce.OrcInputFormat;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class ReadORC extends Configured implements Tool {

	public class Commands {
		@Parameter(names = { "-in" }, description = "Input file or directory")
		private String in;
		@Parameter(names = { "-schema" }, description = "Read projection schema")
		private String schema;
		@Parameter(names = { "-schemaFile" }, description = "Read projection schema")
		private String schemaFile;
		@Parameter(names = { "-out" }, description = "Output directory")
		private String out;
		@Parameter(names = { "-columns" }, description = "Columns to read. Ex. '1,2,3' for columns 1, 2, and 3")
		private String cols;
	}

	public int run(String[] args) throws Exception {
		Commands commands = new Commands();
		new JCommander(commands, args);

		final String schemaString;
		if (commands.schema == null) {
			schemaString = IOUtils.toString(new FileInputStream(new File(commands.schemaFile)));
		} else {
			schemaString = commands.schema;
		}

		OrcConf.MAPRED_OUTPUT_SCHEMA.setString(getConf(), schemaString);
		getConf().setInt(OrcConf.ROW_INDEX_STRIDE.getAttribute(), 1000);
		getConf().set(OrcConf.INCLUDE_COLUMNS.getAttribute(), commands.cols);

		Job job = Job.getInstance(getConf());
		job.setJobName("cff-read-orc");
		job.setJarByClass(ReadORC.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(CountORCMapper.class);

		job.setInputFormatClass(OrcInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(0);

		Path inputFilePath = new Path(commands.in);
		Path outputFilePath = new Path(commands.out);

		FileInputFormat.setInputDirRecursive(job, true);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new ReadORC(), args);
		System.exit(res);
	}
}