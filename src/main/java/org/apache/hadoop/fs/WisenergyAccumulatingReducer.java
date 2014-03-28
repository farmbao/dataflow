package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WisenergyAccumulatingReducer extends Reducer<Text, Text, Text, Text> implements Configurable {

	static final String VALUE_TYPE_LONG = "l:";
	static final String VALUE_TYPE_FLOAT = "f:";
	static final String VALUE_TYPE_STRING = "s:";
	private static final Log LOG = LogFactory.getLog(WisenergyAccumulatingReducer.class);

	protected String hostName;

	Configuration conf;

	public WisenergyAccumulatingReducer() {
		LOG.info("Starting AccumulatingReducer !!!");
		try {
			hostName = java.net.InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			hostName = "localhost";
		}
		LOG.info("Starting AccumulatingReducer on " + hostName);
	}

	@Override
	public void setConf(Configuration c) {
		conf = c;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String field = key.toString();

		context.setStatus("starting " + field + " ::host = " + hostName);
		Iterator<Text> vi = values.iterator();
		// concatenate strings
		if (field.startsWith(VALUE_TYPE_STRING)) {
			StringBuffer sSum = new StringBuffer();

			while (vi.hasNext())
				sSum.append(vi.next().toString()).append(";");
			context.write(key, new Text(sSum.toString()));
			context.setStatus("finished " + field + " ::host = " + hostName);
			return;
		}
		// sum long values
		if (field.startsWith(VALUE_TYPE_FLOAT)) {
			float fSum = 0;
			while (vi.hasNext())
				fSum += Float.parseFloat(vi.next().toString());
			context.write(key, new Text(String.valueOf(fSum)));
			context.setStatus("finished " + field + " ::host = " + hostName);
			return;
		}
		// sum long values
		if (field.startsWith(VALUE_TYPE_LONG)) {
			long lSum = 0;
			while (vi.hasNext()) {
				lSum += Long.parseLong(vi.next().toString());
			}
			context.write(key, new Text(String.valueOf(lSum)));
		}
		context.setStatus("finished " + field + " ::host = " + hostName);
	}

}
