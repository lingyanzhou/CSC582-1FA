package edu.clu.cs;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class TripDataInputFormat extends
		FileInputFormat<LongWritable, TripDataTuple> {

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		CompressionCodec codec =
		new CompressionCodecFactory(context.getConfiguration()).getCodec(filename);

		return codec == null;
	}

	@Override
	public RecordReader<LongWritable, TripDataTuple> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {

		return new TaxiTripRecordReader();
	}

}
