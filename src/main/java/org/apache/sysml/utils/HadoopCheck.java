package org.apache.sysml.utils;

public class HadoopCheck {

	/**
	 * Is Hadoop 'conf' available?
	 *
	 * @return true if available
	 */
	public static boolean confAvailable() {
		try {
			Class.forName("org.apache.hadoop.conf.Configuration");
			return true;
		} catch (ClassNotFoundException e) {
			return false;
		}
	}

	/**
	 * Is Hadoop 'fs' available?
	 *
	 * @return true if available
	 */
	public static boolean fsAvailable() {
		try {
			Class.forName("org.apache.hadoop.fs.FileSystem");
			return true;
		} catch (ClassNotFoundException e) {
			return false;
		}
	}

}
