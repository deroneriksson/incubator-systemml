package org.apache.sysml.utils;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.sysml.conf.HadoopConfigurationManager;

public class HadoopUtils {

	/**
	 * Obtain FileSystem from Hadoop cached JobConf
	 *
	 * @return the FileSystem object
	 * @throws IOException
	 *             if problem occurs obtaining FileSystem from cached JobConf
	 */
	public static FileSystem getFileSystemFromHadoopJobConf() throws IOException {
		FileSystem fs = FileSystem.get(HadoopConfigurationManager.getCachedJobConf());
		return fs;
	}

}
