package edu.clu.cs;

import java.io.IOException;
import java.util.Calendar;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DailyEnsembleNSHourlyBinMapper extends
		Mapper<LongWritable, TripDataTuple, Text, NumericalSummaryTuple> {
	private static final int binNum = 7 * 24;
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
			for (int j = 0; j < 24; ++j) {
				m_keys[getArrayIndex(i, j)] = new Text(colStrs[i] + ","
						+ hourStrs[j]);
			}

		}
	}

	private static int getArrayIndex(int id, int hour) {
		return id * 24 + hour;
	}

	public DailyEnsembleNSHourlyBinMapper() {
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
		final int hour = value.getPickupDatetime().get(Calendar.HOUR_OF_DAY);
		m_vals[getArrayIndex(0, hour)].putRecord(1);
		m_vals[getArrayIndex(1, hour)].putRecord(value.getTripDistance());
		m_vals[getArrayIndex(2, hour)].putRecord(value.getTripTimeInSecs());
		if (0 != value.getTripTimeInSecs()) {
			m_vals[getArrayIndex(3, hour)].putRecord(value.getTripDistance()
					/ value.getTripTimeInSecs());
		} else {
			m_vals[getArrayIndex(3, hour)].putRecord(Float.NaN);
		}
		m_vals[getArrayIndex(4, hour)].putRecord(value.getTipAmount());
		m_vals[getArrayIndex(5, hour)].putRecord(value.getTotalAmount());
		if (0 != value.getTotalAmount()) {
			m_vals[getArrayIndex(6, hour)].putRecord(value.getTipAmount()
					/ value.getTotalAmount());
		} else {
			m_vals[getArrayIndex(3, hour)].putRecord(Float.NaN);
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
