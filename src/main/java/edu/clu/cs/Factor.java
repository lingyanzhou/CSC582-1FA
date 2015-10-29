package edu.clu.cs;

public class Factor {
	public static final float s_NA = Float.NaN;
	protected String[] m_levels = null;

	public Factor(String[] levels) {
		m_levels = levels;
	}
	
	public float convertToFloat(String str) {
		for (int i=0; i<m_levels.length; ++i) {
			if (str.equals(m_levels)) {
				return i;
			}
		}
		return Float.NaN;
	}

	public String convertToString(float val) {
		if (val==m_levels.length) {
			return "";
		}
		return m_levels[Math.round(val)];
	}

	public String[] getLevels() {
		return m_levels;
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