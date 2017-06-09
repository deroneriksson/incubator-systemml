package org.apache.sysml.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sysml.runtime.io.IOUtilFunctions;

public class HDFSUtils {

	public static BufferedReader hadoopPathToBufferedReader(String path) throws IOException {
		Path hdfsPath = new Path(path);
		FileSystem fs = IOUtilFunctions.getFileSystem(hdfsPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(hdfsPath)));
		return br;
	}
}
