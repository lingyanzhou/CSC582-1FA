package edu.clu.cs;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopKItemsLFMapper extends
		Mapper<LongWritable, FloatWritable, LongWritable, FloatWritable> {
	private TreeMap<Float, LongWritable> m_map = new TreeMap<Float, LongWritable>();
	private FloatWritable m_value = new FloatWritable();
	private static final String s_configKString = "TopKItemLFMapper_K";
	private int m_k = 100;

	public static void setK(Configuration conf, int k) {
		conf.setInt(s_configKString, k);
	}

	@Override
	public void setup(Context context) {
		m_k = context.getConfiguration().getInt(s_configKString, 100);
	}

	@Override
	public void map(LongWritable key, FloatWritable value, Context context)
			throws IOException, InterruptedException {
		m_map.put(new Float(value.get()), key);
		if (m_map.size() > m_k) {
			m_map.remove(m_map.firstKey());
		}
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Float k : m_map.keySet()) {
			m_value.set(k.floatValue());
			context.write(m_map.get(k), m_value);
		}

	}
}
