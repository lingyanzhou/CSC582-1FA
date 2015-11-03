package edu.clu.cs;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FloatAverageReducer extends
		Reducer<Text, FloatAverageTuple, Text, FloatAverageTuple> {
	private FloatAverageTuple m_value = new FloatAverageTuple();


	@Override
	public void reduce(Text key, Iterable<FloatAverageTuple> values, Context context)
			throws IOException, InterruptedException {
		m_value.clear();
		for (FloatAverageTuple value : values) { // The iteration will go only once
			m_value.putPartialResult(value);
		}
		context.write(key, m_value);
	}


}
