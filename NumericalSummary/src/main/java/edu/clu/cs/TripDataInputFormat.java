package edu.clu.cs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class TripDataInputFormat extends FileInputFormat<LongWritable, TripDataTuple> {

	@Override
	public RecordReader<LongWritable, TripDataTuple> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		
		return new TaxiTripRecordReader();
	}

}
