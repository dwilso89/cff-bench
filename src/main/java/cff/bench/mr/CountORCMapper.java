package cff.bench.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.mapred.OrcStruct;

public class CountORCMapper extends Mapper<NullWritable, OrcStruct, NullWritable, LongWritable> {

	Long rows = 0L;

	@Override
	public void map(NullWritable key, OrcStruct value, Context context) {
		rows++;
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(), new LongWritable(rows));
	}

}
