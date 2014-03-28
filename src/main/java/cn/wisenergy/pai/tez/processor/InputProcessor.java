package cn.wisenergy.pai.tez.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalIOProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.TezProcessorContext;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.VertexManagerEventPayloadProto;

import cn.wisenergy.pai.hadoop2.Constants;
import cn.wisenergy.pai.hadoop2.seismic.SegyUtil;
import cn.wisenergy.pai.hadoop2.seismic.TraceDataWritable;
import cn.wisenergy.pai.hadoop2.seismic.TraceHeaderFormat;
import cn.wisenergy.pai.hadoop2.seismic.TraceHeaderWritable;
import cn.wisenergy.pai.tez.EventHelper;

import com.google.common.base.Preconditions;

public class InputProcessor implements LogicalIOProcessor {

	private static final Log LOG = LogFactory.getLog(InputProcessor.class);
	private TezCounters counters;
	private TezProcessorContext context;
	@Override
	public void initialize(TezProcessorContext processorContext) throws Exception {
		LOG.info("the payload length is :"+processorContext.getUserPayload().length);
		MRInputUserPayloadProto userPayloadProto = MRHelpers
		        .parseMRInputPayload(processorContext.getUserPayload());
		Configuration conf = MRHelpers.createConfFromByteString(userPayloadProto
		        .getConfigurationBytes());
		LOG.info("paritioner is "+conf.get(MRJobConfig.PARTITIONER_CLASS_ATTR));
		counters = processorContext.getCounters();
		context= processorContext;
	}

	@Override
	public void handleEvents(List<Event> processorEvents) {

	}

	@Override
	public void close() throws Exception {
	}

	@Override
	public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {
		for (LogicalInput input : inputs.values()) {
			input.start();
		}
		for (LogicalOutput output : outputs.values()) {
			output.start();
		}
		Preconditions.checkArgument(inputs.size() == 1);
		Preconditions.checkArgument(outputs.size() == 1);
		
		
		KeyValueReader kvReader = (KeyValueReader) inputs.values().iterator().next().getReader();
		int i=0;
		
		while (kvReader.next()) {
			TraceHeaderWritable inKey = (TraceHeaderWritable) kvReader.getCurrentKey();
			TraceDataWritable inValue = (TraceDataWritable) kvReader.getCurrentValue();
			TraceHeaderWritable outKey = new TraceHeaderWritable();
			TraceDataWritable outValue = new TraceDataWritable();
			doWorkflow(inKey, inValue, outKey, outValue);
			Iterator it=outputs.values().iterator();
			while(it.hasNext()){
				OnFileSortedOutput output = (OnFileSortedOutput)it.next();
				KeyValueWriter kvWriter = (KeyValueWriter) output.getWriter();
				writeWithNewKeys(kvWriter,outKey,outValue);
				
			}
			i++;
			if(i%1000==0){
				CounterGroup cgroup=counters.getGroup("DataFlow");
				TezCounter counter=cgroup.findCounter(context.getTaskVertexName());
				counter.increment(1000);
				EventHelper.sentVertexManagerEvent(context, 1000);
			}
		}
		CounterGroup cgroup=counters.getGroup("DataFlow");
		TezCounter counter=cgroup.findCounter(context.getTaskVertexName());
		counter.increment(i%1000);
		EventHelper.sentVertexManagerEvent(context, i%1000);
	}

	public void doWorkflow(TraceHeaderWritable inKey, TraceDataWritable inValue, TraceHeaderWritable outKey, TraceDataWritable outValue) {
		outKey.setHeaderBytes(inKey.getHeaderBytes());
		
		outValue.setDataBytes(inValue.getDataBytes());
	}
	
	private void writeWithNewKeys(KeyValueWriter writer,TraceHeaderWritable h,TraceDataWritable d) throws IOException{
		int[] keys = new int[3];
		SegyUtil util = new SegyUtil(h.getHeaderBytes(), false);
		keys[0] = util.readInt(TraceHeaderFormat.m_InLineNo);
		keys[1] = util.readInt(TraceHeaderFormat.m_CrossLineNo);
		keys[2] = util.readInt(TraceHeaderFormat.m_Offset);
		h.setKeys(keys);
		writer.write(h, d);
	}

}
