package edu.clu.cs;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKItemsReducer extends
		Reducer<Text, FloatWritable, Text, FloatWritable> {
	private TreeMap<Float, Text> m_map = new TreeMap<Float, Text>();
	private FloatWritable m_value = new FloatWritable();
	private static final String s_configKString = "TopKItemsReducer_k";
	private int m_k=100;

	public static void setK(Configuration conf, int k) {
		conf.setInt(s_configKString, k);
	}
	
	@Override
	public void setup(Context context) {
		m_k = context.getConfiguration().getInt(s_configKString, 100);
	}

	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {
		float sum = 0;
		for (FloatWritable value : values) { // The iteration will go only once
			sum += value.get();
		}
		m_map.put(new Float(sum), new Text(key));
		if (m_map.size() > m_k) {
			m_map.remove(m_map.firstKey());
		}
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Float k : m_map.keySet()) {
			//m_value.set(m_k);
			m_value.set(k.floatValue());
			context.write(m_map.get(k), m_value);
		}

	}
}
