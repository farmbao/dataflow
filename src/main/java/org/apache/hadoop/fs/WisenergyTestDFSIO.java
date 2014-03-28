package org.apache.hadoop.fs;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WisenergyTestDFSIO extends Configured implements Tool {
	// Constants
	private static final Log LOG = LogFactory.getLog(WisenergyTestDFSIO.class);
	
	private static final String BASE_FILE_NAME = "test_io_";
	private UserArguments args;
	private static final long MEGA = 0x100000;
	private static String TEST_ROOT_DIR = System.getProperty("test.build.data", "/benchmarks/TestDFSIO");
	private static Path WRITE_DIR = new Path(TEST_ROOT_DIR, "io_write");
	private static Path READ_DIR = new Path(TEST_ROOT_DIR, "io_read");
	public static Path DATA_DIR = new Path(TEST_ROOT_DIR, "io_data");
	
	static {
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
	}

	public static String getFileName(int fIdx) {
		return BASE_FILE_NAME + Integer.toString(fIdx);
	}


	/**
	 * Write mapper class.
	 */
	public static class WriteMapper extends WisenergyIOMapperBase<Long> {
		
		void collectStats(Context output, String name, long execTime, Long objSize)
				throws IOException, InterruptedException {
			long totalSize = objSize.longValue();
			float ioRateMbSec = (float) totalSize * 1000 / (execTime * MEGA);
			LOG.info("Number of bytes processed = " + totalSize);
			LOG.info("Exec time = " + execTime);
			LOG.info("IO rate = " + ioRateMbSec);

			output.write(new Text(WisenergyAccumulatingReducer.VALUE_TYPE_LONG + "tasks"), new Text(String.valueOf(1)));
			output.write(new Text(WisenergyAccumulatingReducer.VALUE_TYPE_LONG + "size"), new Text(String.valueOf(totalSize)));
			output.write(new Text(WisenergyAccumulatingReducer.VALUE_TYPE_LONG + "time"), new Text(String.valueOf(execTime)));
			output.write(new Text(WisenergyAccumulatingReducer.VALUE_TYPE_FLOAT + "rate"),
					new Text(String.valueOf(ioRateMbSec * 1000)));
			output.write(new Text(WisenergyAccumulatingReducer.VALUE_TYPE_FLOAT + "sqrate"),
					new Text(String.valueOf(ioRateMbSec * ioRateMbSec * 1000)));
		}
		public WriteMapper() {
			for (int i = 0; i < bufferSize; i++)
				buffer[i] = (byte) ('0' + i % 50);
		}

		public Long doIO(Context reporter, String name, long totalSize) throws IOException {
			// create file
			totalSize *= MEGA;
			OutputStream out;
			out = fs.create(new Path(DATA_DIR, name), true, bufferSize);

			try {
				// write to the file
				long nrRemaining;
				for (nrRemaining = totalSize; nrRemaining > 0; nrRemaining -= bufferSize) {
					int curSize = (bufferSize < nrRemaining) ? bufferSize : (int) nrRemaining;
					out.write(buffer, 0, curSize);
					reporter.setStatus("writing " + name + "@" + (totalSize - nrRemaining) + "/" + totalSize
							+ " ::host = " + hostName);
				}
			} finally {
				out.close();
			}
			return Long.valueOf(totalSize);
		}

	}

	private void writeTest(FileSystem fs, Configuration fsConfig) throws IOException {

		fs.delete(DATA_DIR, true);
		fs.delete(WRITE_DIR, true);

		runIOTest(WriteMapper.class, WRITE_DIR, fsConfig);
	}

	private void runIOTest(Class<? extends Mapper<Text, LongWritable, Text, Text>> mapperClass, Path outputDir,
			Configuration fsConfig) throws IOException {
		Job job = Job.getInstance(fsConfig);
		job.setJarByClass(WisenergyTestDFSIO.class);
		job.getConfiguration().setBoolean("mapred.mapper.new-api", true);
		job.getConfiguration().setBoolean("mapred.reducer.new-api", true);
		
		job.setMapperClass(mapperClass);
		job.setReducerClass(WisenergyAccumulatingReducer.class);
		job.setInputFormatClass(TestDFSIOInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setNumReduceTasks(1);
		try {
			job.waitForCompletion(true);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Read mapper class.
	 */
	public static class ReadMapper extends WisenergyIOMapperBase<Long> {

		public ReadMapper() {
		}
		void collectStats(Context output, String name, long execTime, Long objSize)
				throws IOException, InterruptedException {
			long totalSize = objSize.longValue();
			float ioRateMbSec = (float) totalSize * 1000 / (execTime * MEGA);
			LOG.info("Number of bytes processed = " + totalSize);
			LOG.info("Exec time = " + execTime);
			LOG.info("IO rate = " + ioRateMbSec);

			output.write(new Text(WisenergyAccumulatingReducer.VALUE_TYPE_LONG + "tasks"), new Text(String.valueOf(1)));
			output.write(new Text(WisenergyAccumulatingReducer.VALUE_TYPE_LONG + "size"), new Text(String.valueOf(totalSize)));
			output.write(new Text(WisenergyAccumulatingReducer.VALUE_TYPE_LONG + "time"), new Text(String.valueOf(execTime)));
			output.write(new Text(WisenergyAccumulatingReducer.VALUE_TYPE_FLOAT + "rate"),
					new Text(String.valueOf(ioRateMbSec * 1000)));
			output.write(new Text(WisenergyAccumulatingReducer.VALUE_TYPE_FLOAT + "sqrate"),
					new Text(String.valueOf(ioRateMbSec * ioRateMbSec * 1000)));
		}
		public Long doIO(Context reporter, String name, long totalSize) throws IOException {
			totalSize *= MEGA;
			// open file
			DataInputStream in = fs.open(new Path(DATA_DIR, name));
			long actualSize = 0;
			try {
				while (actualSize < totalSize) {
					int curSize = in.read(buffer, 0, bufferSize);
					if (curSize < 0)
						break;
					actualSize += curSize;
					reporter.setStatus("reading " + name + "@" + actualSize + "/" + totalSize + " ::host = " + hostName);
				}
			} finally {
				in.close();
			}
			return Long.valueOf(actualSize);
		}
	}

	private void readTest(FileSystem fs, Configuration fsConfig) throws IOException {
		fs.delete(READ_DIR, true);
		runIOTest(ReadMapper.class, READ_DIR, fsConfig);
	}

	private static void sequentialTest(FileSystem fs, Command testType, int fileSize, int nrFiles) throws Exception {
		WisenergyIOMapperBase<Long> ioer = null;
		if (testType == Command.READ)
			ioer = new ReadMapper();
		else if (testType == Command.WRITE)
			ioer = new WriteMapper();
		else
			return;
		for (int i = 0; i < nrFiles; i++)
			//TODO: should modify null for support sequential
			ioer.doIO(null, BASE_FILE_NAME + Integer.toString(i), MEGA * fileSize);
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new WisenergyTestDFSIO(), args);
		System.exit(res);
	}

	private static void analyzeResult(FileSystem fs, Command testType, long execTime, String resFileName)
			throws IOException {
		Path reduceFile;
		if (testType == Command.WRITE)
			reduceFile = new Path(WRITE_DIR, "part-r-00000");
		else
			reduceFile = new Path(READ_DIR, "part-r-00000");
		long tasks = 0;
		long size = 0;
		long time = 0;
		float rate = 0;
		float sqrate = 0;
		DataInputStream in = null;
		BufferedReader lines = null;
		try {
			in = new DataInputStream(fs.open(reduceFile));
			lines = new BufferedReader(new InputStreamReader(in));
			String line;
			while ((line = lines.readLine()) != null) {
				StringTokenizer tokens = new StringTokenizer(line, " \t\n\r\f%");
				String attr = tokens.nextToken();
				if (attr.endsWith(":tasks"))
					tasks = Long.parseLong(tokens.nextToken());
				else if (attr.endsWith(":size"))
					size = Long.parseLong(tokens.nextToken());
				else if (attr.endsWith(":time"))
					time = Long.parseLong(tokens.nextToken());
				else if (attr.endsWith(":rate"))
					rate = Float.parseFloat(tokens.nextToken());
				else if (attr.endsWith(":sqrate"))
					sqrate = Float.parseFloat(tokens.nextToken());
			}
		} finally {
			if (in != null)
				in.close();
			if (lines != null)
				lines.close();
		}

		double med = rate / 1000 / tasks;
		double stdDev = Math.sqrt(Math.abs(sqrate / 1000 / tasks - med * med));
		String resultLines[] = {
				"----- TestDFSIO ----- : "
						+ ((testType == Command.WRITE) ? "write" : (testType == Command.READ) ? "read" : "unknown"),
				"           Date & time: " + new Date(System.currentTimeMillis()), "       Number of files: " + tasks,
				"Total MBytes processed: " + size / MEGA, "     Throughput mb/sec: " + size * 1000.0 / (time * MEGA),
				"Average IO rate mb/sec: " + med, " IO rate std deviation: " + stdDev,
				"    Test exec time sec: " + (float) execTime / 1000, "" };

		PrintStream res = null;
		try {
			res = new PrintStream(new FileOutputStream(new File(resFileName), true));
			for (int i = 0; i < resultLines.length; i++) {
				LOG.info(resultLines[i]);
				res.println(resultLines[i]);
			}
		} finally {
			if (res != null)
				res.close();
		}
	}

	private static void cleanup(FileSystem fs) throws IOException {
		LOG.info("Cleaning up test files");
		fs.delete(new Path(TEST_ROOT_DIR), true);
	}

	@Override
	public int run(String[] args) throws Exception {
		this.args=new UserArguments();

		String className = WisenergyTestDFSIO.class.getSimpleName();
		String version = className + ".0.0.4";
		String usage = "Usage: " + className + " -read | -write | -clean "
				+ "[-framework (yarn|yran-tez)][-nrFiles N] [-fileSize MB] [-resFile resultFileName] " + "[-bufferSize Bytes] [-replication replica] ";

		System.out.println(version);
		if (args.length == 0) {
			System.err.println(usage);
			return -1;
		}
		for (int i = 0; i < args.length; i++) { // parse command line
			if (args[i].startsWith("-read")) {
				this.args.setCmd(Command.READ);
			} else if (args[i].equals("-write")) {
				this.args.setCmd(Command.WRITE);
			} else if (args[i].equals("-clean")) {
				this.args.setCmd(Command.CLEAN);
			} else if (args[i].startsWith("-seq")) {
				this.args.setSeq(true);
			} else if (args[i].equals("-nrFiles")) {
				this.args.setNrFiles(Integer.parseInt(args[++i]));
			} else if (args[i].equals("-fileSize")) {
				this.args.setFileSize(Integer.parseInt(args[++i]));
			} else if (args[i].equals("-bufferSize")) {
				this.args.setBufferSize(Integer.parseInt(args[++i]));
			} else if (args[i].equals("-resFile")) {
				this.args.setResFile(args[++i]);
			} else if (args[i].equals("-replication")){
				this.args.setReplication(Integer.parseInt(args[++i]));
			} else if (args[i].equals("-framework")){
				this.args.setFramework(args[++i]);
			}
		}

		LOG.info(this.args);

		try {
			Configuration fsConfig = new Configuration(getConf());
			fsConfig.setInt("test.io.file.buffer.size",this.args.getBufferSize());
			fsConfig.set("mapreduce.framework.name",this.args.getFramework());
			fsConfig.setInt("dfs.replication", this.args.getReplication());
			fsConfig.set("test.data.dir", DATA_DIR.toString());
			fsConfig.set("test.result.file", this.args.getResFile());
			fsConfig.setEnum("test.cmd",this.args.getCmd());
			fsConfig.setInt("test.fileSize", this.args.getFileSize());
			fsConfig.setInt("test.nrFiles", this.args.getNrFiles());
			
			LOG.info("the namenode is "+fsConfig.get("fs.defaultFS"));
			FileSystem fs = FileSystem.get(fsConfig);

			if (this.args.isSeq()) {
				long tStart = System.currentTimeMillis();
				sequentialTest(fs, this.args.getCmd(), this.args.getFileSize(), this.args.getNrFiles());
				long execTime = System.currentTimeMillis() - tStart;
				String resultLine = "Seq Test exec time sec: " + (float) execTime / 1000;
				LOG.info(resultLine);
				return 0;
			}
			if (this.args.getCmd()==Command.CLEAN) {
				cleanup(fs);
				return 0;
			}
			long tStart = System.currentTimeMillis();
			if (this.args.getCmd() == Command.WRITE)
				writeTest(fs, fsConfig);
			if (this.args.getCmd() == Command.READ)
				readTest(fs, fsConfig);
			long execTime = System.currentTimeMillis() - tStart;

			analyzeResult(fs, this.args.getCmd(), execTime, this.args.getResFile());
		} catch (Exception e) {
			System.err.print(StringUtils.stringifyException(e));
			return -1;
		}
		return 0;
	}

}
