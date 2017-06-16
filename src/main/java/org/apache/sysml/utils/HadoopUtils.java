package org.apache.sysml.utils;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.sysml.conf.HadoopConfigurationManager;

public class HadoopUtils {

	public static FileSystem getFileSystemFromHadoopJobConf() throws IOException {
		FileSystem fs = FileSystem.get(HadoopConfigurationManager.getCachedJobConf());
		return fs;
	}

}
