package edu.clu.cs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class BasicTripDataFilterMapper extends
		Mapper<LongWritable, TripDataTuple, LongWritable, TripDataTuple> {
	@Override
	public void map(LongWritable key, TripDataTuple value, Context context)
			throws IOException, InterruptedException {
		if (!value.containsNA()) {
			context.write(key, value);
		}
	}

}
