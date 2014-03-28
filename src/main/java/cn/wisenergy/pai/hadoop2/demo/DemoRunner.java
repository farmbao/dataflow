package cn.wisenergy.pai.hadoop2.demo;

import java.io.FileNotFoundException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.WisenergyTestDFSIO;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.ToolRunner;

public class DemoRunner {
	private final static Log LOG=LogFactory.getLog(DemoRunner.class);
	public final static Path SYSTEM_LIB_DIR=new Path("/lib");
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		FileSystem fs=FileSystem.get(new Configuration());
		
		int res=0;
		for (int i = 0; i < args.length; i++) { // parse command line
			if (args[i].trim().toLowerCase().startsWith("dfsio")) {
				res = ToolRunner.run(new WisenergyTestDFSIO(), args);
			} else if (args[i].trim().toLowerCase().equals("dataflow")) {
				LOG.info("run Tez DataFlow demo");
				res = ToolRunner.run(new DataFlow(), args);
			}
		}
		System.exit(res);
	}
}
