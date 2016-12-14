package cff.bench.mr;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class CountRowsMapper extends Mapper<Void, GenericRecord, NullWritable, LongWritable> {

	Long rows = 0L;

	@Override
	public void map(Void key, GenericRecord value, Context context) {
		rows++;
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(), new LongWritable(rows));
	}

}
