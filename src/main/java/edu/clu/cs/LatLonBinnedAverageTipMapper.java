package edu.clu.cs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class LatLonBinnedAverageTipMapper extends
		Mapper<LongWritable, TripDataTuple, Text, FloatAverageTuple> {
	private Text m_key = new Text();
	private Map<String, FloatAverageTuple> m_map = new HashMap<String, FloatAverageTuple>();
	private static final String s_configDivisionString = "LatLonBinnedAverageTipMapper_division";
	private int m_division = 1000;
	private float m_latitudeBinWidth = 0;
	private float m_longitudeBinWidth = 0;

	public static void setDivision(Configuration conf, int division) {
		conf.setInt(s_configDivisionString, division);
	}

	@Override
	public void setup(Context context) {
		m_division = context.getConfiguration().getInt(s_configDivisionString,
				10000);
		m_longitudeBinWidth = calculateBinWidth(
				TripDataFormatHelper.s_pickupLongitude.getMin(),
				TripDataFormatHelper.s_pickupLongitude.getMax(), m_division);
		m_latitudeBinWidth = calculateBinWidth(
				TripDataFormatHelper.s_pickupLatitude.getMin(),
				TripDataFormatHelper.s_pickupLatitude.getMax(), m_division);
	}

	@Override
	public void map(LongWritable key, TripDataTuple value, Context context)
			throws IOException, InterruptedException {
		String k = makeKey(value);
		if (m_map.containsKey(k)) {
			m_map.get(k).putRecord(value.getTipAmount());
		} else {
			FloatAverageTuple tmp = new FloatAverageTuple();
			tmp.putRecord(value.getTipAmount());
			m_map.put(k, tmp);
		}
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		for (String k : m_map.keySet()) {
			m_key.set(k);
			context.write(m_key, m_map.get(k));
		}

	}
	
	private float calculateBinWidth(float min, float max, int div) {
		return (max - min) / div;
	}

	private String getLongitudeBinName(float val) {
		int tmp = (int)((val-TripDataFormatHelper.s_pickupLongitude.getMin())/m_longitudeBinWidth);
		return "" + (TripDataFormatHelper.s_pickupLongitude.getMin()+(tmp+0.5)*m_longitudeBinWidth);
	}

	private String getLatitudeBinName(float val) {
		int tmp = (int)((val-TripDataFormatHelper.s_pickupLatitude.getMin())/m_latitudeBinWidth);
		return "" + (TripDataFormatHelper.s_pickupLatitude.getMin()+(tmp+0.5)*m_latitudeBinWidth);
		
	}
	
	private String makeKey(TripDataTuple value) {
		String ret = "";
		ret += getLongitudeBinName(value.getPickupLongitude());
		ret += ",";
		ret += getLatitudeBinName(value.getPickupLatitude());
		return ret;
	}
}
