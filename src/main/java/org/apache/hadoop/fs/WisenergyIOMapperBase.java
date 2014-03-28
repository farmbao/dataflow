/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Base mapper class for IO operations.
 * <p>
 * Two abstract method {@link #doIO(Reporter, String, long)} and
 * {@link #collectStats(OutputCollector,String,long,Object)} should be
 * overloaded in derived classes to define the IO operation and the statistics
 * data to be collected by subsequent reducers.
 * 
 */
public abstract class WisenergyIOMapperBase<T> extends Mapper<Text, LongWritable, Text, Text> implements Configurable {

	private static final Log LOG = LogFactory.getLog(WisenergyIOMapperBase.class);
	protected byte[] buffer;
	protected int bufferSize;
	protected FileSystem fs;
	protected String hostName;
	protected Closeable stream;
	private Configuration conf;

	public WisenergyIOMapperBase() {
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public Configuration getConf() {
		return conf;
	}

	private void configure(Configuration conf) {
		if (conf != null) {
			setConf(conf);
		}

		try {
			fs = FileSystem.get(conf);
		} catch (Exception e) {
			throw new RuntimeException("Cannot create file system.", e);
		}
		bufferSize = conf.getInt("test.io.file.buffer.size", 4096);
		buffer = new byte[bufferSize];
		try {
			hostName = InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			hostName = "localhost";
		}
	}

	/**
	 * Perform io operation, usually read or write.
	 * 
	 * @param reporter
	 * @param name
	 *            file name
	 * @param value
	 *            offset within the file
	 * @return object that is passed as a parameter to
	 *         {@link #collectStats(OutputCollector,String,long,Object)}
	 * @throws IOException
	 */
	abstract T doIO(Context reporter, String name, long value) throws IOException;

	/**
	 * Collect stat data to be combined by a subsequent reducer.
	 * 
	 * @param output
	 * @param name
	 *            file name
	 * @param execTime
	 *            IO execution time
	 * @param doIOReturnValue
	 *            value returned by {@link #doIO(Reporter,String,long)}
	 * @throws IOException
	 */
	abstract void collectStats(Context output, String name, long execTime, T doIOReturnValue) throws IOException,
			InterruptedException;

	public void map(Text key, LongWritable value, Context context) throws IOException {
		configure(context.getConfiguration());
		String name = key.toString();
		long longValue = value.get();
		context.setStatus("starting " + name + " ::host = " + hostName);

		T statValue = null;
		long tStart = System.currentTimeMillis();
		LOG.info("the file name is " + name);
		LOG.info("the fileSize is " + longValue);
		statValue = doIO(context, name, longValue);
		long tEnd = System.currentTimeMillis();
		long execTime = tEnd - tStart;
		try {
			collectStats(context, name, execTime, statValue);
		} catch (InterruptedException e) {
			LOG.error(e);
		}

		context.setStatus("finished " + name + " ::host = " + hostName);
	}
}
