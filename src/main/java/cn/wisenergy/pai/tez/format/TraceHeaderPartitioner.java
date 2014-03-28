package cn.wisenergy.pai.tez.format;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Partitioner;

import cn.wisenergy.pai.hadoop2.seismic.TraceDataWritable;
import cn.wisenergy.pai.hadoop2.seismic.TraceHeaderWritable;

public class TraceHeaderPartitioner extends Partitioner<TraceHeaderWritable,TraceDataWritable>{
	
	private static final Log LOG=LogFactory.getLog(TraceHeaderPartitioner.class);
	public TraceHeaderPartitioner(){
	}
	@Override
	public int getPartition(TraceHeaderWritable key, TraceDataWritable v, int numPartitions) {
//		LOG.info("the key in partitionor :"+Arrays.toString(key.getKeys()));
		return new Random().nextInt(numPartitions);
	}

}
