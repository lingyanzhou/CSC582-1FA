package edu.clu.cs;

public class Factor {
	public static final float s_NA = Float.NaN;
	protected String[] m_levels = null;
	protected String[] m_labels = null;

	public Factor(String[] levels) {
		m_levels = levels;
		m_labels = m_levels;
	}
	
	public Factor(String[] levels, String[] labels) {
		assert(levels.length == labels.length);
		m_levels = levels;
		m_labels = labels;
	}
	
	public float convertToFloat(String str) {
		for (int i=0; i<m_levels.length; ++i) {
			if (str.equals(m_levels[i])) {
				return i;
			}
		}
		return Float.NaN;
	}

	public String convertToString(float val) {
		int index = Math.round(val);
		if (index>=m_levels.length || index < 0) {
			return "_NA_";
		}
		return m_labels[index];
	}

	public String[] getLevels() {
		return m_levels;
	}

	public String[] getLabels() {
		return m_labels;
	}
	
	public boolean isMissing(String str) {
		for (int i=0; i<m_levels.length; ++i) {
			if (str.equals(m_levels)) {
				return false;
			}
		}
		return true;
	}
//	
//	public boolean isMissing(int val) {
//		return val>=m_levels.length || val <0;
//	}
}