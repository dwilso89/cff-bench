package cff.bench.mr;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;

import org.apache.avro.Schema;
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
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageTypeParser;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class ReadParquet extends Configured implements Tool {

	public class Commands {
		@Parameter(names = { "-in" }, description = "Input file or directory")
		private String in;
		@Parameter(names = { "-schema" }, description = "Read projection schema")
		private String schema;
		@Parameter(names = { "-schemaFile" }, description = "Read projection schema")
		private String schemaFile;
		@Parameter(names = { "-out" }, description = "Output directory")
		private String out;
		@Parameter(names = {
				"-cols" }, description = "Columns to read. Ex. 'ColA,ColB,ColC' for columns ColA, ColB, and Colc")
		private List<String> cols;
	}

	public int run(String[] args) throws Exception {
		Commands commands = new Commands();
		new JCommander(commands, args);

		// for(String col : commands.cols){
		// intColumn(col);
		// }
		// OrcInputFormat.setSearchArgument(conf, sarg, cols.);
		final String schemaString;
		if (commands.schema == null) {
			schemaString = IOUtils.toString(new FileInputStream(new File(commands.schemaFile)));
		} else {
			schemaString = commands.schema;
		}

		final Schema avroProjectedSchema = new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(schemaString));

		Job job = Job.getInstance(getConf());
		job.setJobName("cff-read-parquet");
		job.setJarByClass(ReadParquet.class);

		AvroReadSupport.setRequestedProjection(getConf(), avroProjectedSchema);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(CountRowsMapper.class);

		job.setInputFormatClass(AvroParquetInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(0);

		Path inputFilePath = new Path(commands.in);
		Path outputFilePath = new Path(commands.out);

		FileInputFormat.setInputDirRecursive(job, true);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		// AvroParquetInputFormat.setReadSupportClass(conf, readSupportClass);
		// ParquetInputFormat.setReadSupportClass(job, AvroReadSupport.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new ReadParquet(), args);
		System.exit(res);
	}
}