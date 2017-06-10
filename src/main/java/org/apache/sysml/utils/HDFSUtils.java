package org.apache.sysml.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.xml.parsers.DocumentBuilder;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sysml.runtime.io.IOUtilFunctions;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class HDFSUtils {

	public static BufferedReader hadoopPathToBufferedReader(String path) throws IOException {
		Path hdfsPath = new Path(path);
		FileSystem fs = IOUtilFunctions.getFileSystem(hdfsPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(hdfsPath)));
		return br;
	}

	public static Document hadoopPathToDocument(DocumentBuilder builder, String path) throws IOException, SAXException {
		Path hdfsPath = new Path(path);
		FileSystem fs = IOUtilFunctions.getFileSystem(hdfsPath);
		Document domTree = builder.parse(fs.open(hdfsPath));
		return domTree;
	}
}
