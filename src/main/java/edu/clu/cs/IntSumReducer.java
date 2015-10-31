package edu.clu.cs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable m_val = new IntWritable();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable val : values) {
			count += val.get();
		}
		m_val.set(count);
		context.write(key, m_val);
	}
}
