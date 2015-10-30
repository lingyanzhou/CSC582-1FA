package edu.clu.cs;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class WeeklyEnsembleNSHourlyBinMapper extends
		Mapper<LongWritable, TripDataTuple, Text, NumericalSummaryTuple> {
	private static final int binNum = 7 * 24 * 7;
	private static Text[] m_keys = new Text[binNum];
	private NumericalSummaryTuple[] m_vals = new NumericalSummaryTuple[binNum];

	static {
		String[] colStrs = { "pickup_count", "trip_distance",
				"trip_time_in_secs", "ratio_distance_time", "tip_amount",
				"total_amount", "ratio_tip_total" };
		String[] hourStrs = new String[24];
		for (int i = 0; i < 24; ++i) {
			hourStrs[i] = String.format("%02d", i);
		}

		for (int i = 0; i < 7; ++i) {
			for (int j = 1; j < 8; ++j) {
				for (int k = 0; k < 24; ++k) {
					m_keys[getArrayIndex(i, j, k)] = new Text(colStrs[i] + ","
							+ j + "," + hourStrs[k]);
				}
			}
		}
	}

	private static int getArrayIndex(int id, int day, int hour) {
		return id * 24 * 7 + (day - 1) * 24 + hour;
	}

	public WeeklyEnsembleNSHourlyBinMapper() {
		for (int i = 0; i < binNum; ++i) {
			m_vals[i] = new NumericalSummaryTuple();
		}
	}

	@Override
	public void setup(Context context) {
		for (int i = 0; i < binNum; ++i) {
			m_vals[i].clear();
		}
	}

	@Override
	public void map(LongWritable key, TripDataTuple value, Context context)
			throws IOException, InterruptedException {

		if (null == value.getPickupDatetime()) {
			return;
		}

		final int dayOfWeek = value.getPickupDatetime().get(
				Calendar.DAY_OF_WEEK);
		final int hour = value.getPickupDatetime().get(Calendar.HOUR_OF_DAY);
		m_vals[getArrayIndex(0, dayOfWeek, hour)].putRecord(1);
		m_vals[getArrayIndex(1, dayOfWeek, hour)].putRecord(value
				.getTripDistance());
		m_vals[getArrayIndex(2, dayOfWeek, hour)].putRecord(value
				.getTripTimeInSecs());
		if (0 != value.getTripTimeInSecs()) {
			m_vals[getArrayIndex(3, dayOfWeek, hour)].putRecord(value
					.getTripDistance() / value.getTripTimeInSecs());
		} else {
			m_vals[getArrayIndex(3, dayOfWeek, hour)].putRecord(Float.NaN);
		}
		m_vals[getArrayIndex(4, dayOfWeek, hour)].putRecord(value
				.getTipAmount());
		m_vals[getArrayIndex(5, dayOfWeek, hour)].putRecord(value
				.getTotalAmount());

		if (0 != value.getTotalAmount()) {
			m_vals[getArrayIndex(6, dayOfWeek, hour)].putRecord(value
					.getTipAmount() / value.getTotalAmount());
		} else {
			m_vals[getArrayIndex(6, dayOfWeek, hour)].putRecord(Float.NaN);
		}
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		for (int i = 0; i < binNum; ++i) {
			context.write(m_keys[i], m_vals[i]);
		}
	}
}
