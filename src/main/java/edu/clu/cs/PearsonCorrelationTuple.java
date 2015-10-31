package edu.clu.cs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class PearsonCorrelationTuple implements Writable {
	private long m_totalCount = 0;
	private double m_sumV1 = Double.NaN;
	private double m_squaredSumV1 = Double.NaN;
	private double m_sumV2 = Double.NaN;
	private double m_squaredSumV2 = Double.NaN;
	private double m_multipliedSum = Double.NaN;

	public void clear() {
		m_totalCount = 0;
		m_sumV1 = Double.NaN;
		m_squaredSumV1 = Double.NaN;
		m_sumV2 = Double.NaN;
		m_squaredSumV2 = Double.NaN;
		m_multipliedSum = Double.NaN;
	}

	public void putRecord(float val1, float val2) {

		if (!Float.isNaN(val1) && !Float.isNaN(val2)) {
			m_totalCount += 1;

			if (!Double.isNaN(m_sumV1)) {
				m_sumV1 += val1;
			} else {
				m_sumV1 = val1;
			}

			if (!Double.isNaN(m_squaredSumV1)) {
				m_squaredSumV1 += Math.pow(val1, 2);
			} else {
				m_squaredSumV1 = Math.pow(val1, 2);
			}

			if (!Double.isNaN(m_sumV2)) {
				m_sumV2 += val2;
			} else {
				m_sumV2 = val2;
			}

			if (!Double.isNaN(m_squaredSumV2)) {
				m_squaredSumV2 += Math.pow(val2, 2);
			} else {
				m_squaredSumV2 = Math.pow(val2, 2);
			}

			if (!Double.isNaN(m_multipliedSum)) {
				m_multipliedSum += val1 * val2;
			} else {
				m_multipliedSum = val1 * val2;
			}
		}
	}

	public void putPartialResult(PearsonCorrelationTuple tuple) {
		m_totalCount += tuple.m_totalCount;

		if (!Double.isNaN(tuple.m_sumV1)) {
			if (!Double.isNaN(m_sumV1)) {
				m_sumV1 += tuple.m_sumV1;
			} else {
				m_sumV1 = tuple.m_sumV1;
			}
		}

		if (!Double.isNaN(tuple.m_squaredSumV1)) {
			if (!Double.isNaN(m_squaredSumV1)) {
				m_squaredSumV1 += tuple.m_squaredSumV1;
			} else {
				m_squaredSumV1 = tuple.m_squaredSumV1;
			}

		}

		if (!Double.isNaN(tuple.m_sumV2)) {
			if (!Double.isNaN(m_sumV2)) {
				m_sumV2 += tuple.m_sumV2;
			} else {
				m_sumV2 = tuple.m_sumV2;
			}
		}

		if (!Double.isNaN(tuple.m_squaredSumV2)) {
			if (!Double.isNaN(m_squaredSumV2)) {
				m_squaredSumV2 += tuple.m_squaredSumV2;
			} else {
				m_squaredSumV2 = tuple.m_squaredSumV2;
			}

		}

		if (!Double.isNaN(tuple.m_multipliedSum)) {
			if (!Double.isNaN(m_multipliedSum)) {
				m_multipliedSum += tuple.m_multipliedSum;
			} else {
				m_multipliedSum = tuple.m_multipliedSum;
			}

		}
	}

	private double getResult() {
		return (m_multipliedSum / m_totalCount - m_sumV1 * m_sumV2
				/ Math.pow(m_totalCount, 2))
				/ (Math.sqrt(m_squaredSumV1 / m_totalCount
						- Math.pow(m_sumV1 / m_totalCount, 2)) * Math
							.sqrt(m_squaredSumV2 / m_totalCount
									- Math.pow(m_sumV2 / m_totalCount, 2)));
	}

	public String toString() {
		return "" + getResult();
	}

	public void readFields(DataInput in) throws IOException {
		m_sumV1 = in.readDouble();
		m_squaredSumV1 = in.readDouble();
		m_sumV2 = in.readDouble();
		m_squaredSumV2 = in.readDouble();
		m_multipliedSum = in.readDouble();
		m_totalCount = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(m_sumV1);
		out.writeDouble(m_squaredSumV1);
		out.writeDouble(m_sumV2);
		out.writeDouble(m_squaredSumV2);
		out.writeDouble(m_multipliedSum);
		out.writeLong(m_totalCount);
	}

}
