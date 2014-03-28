package cn.wisenergy.pai.tez.processor;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalIOProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.TezProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;

import cn.wisenergy.pai.hadoop2.seismic.SegyUtil;
import cn.wisenergy.pai.hadoop2.seismic.TraceDataWritable;
import cn.wisenergy.pai.hadoop2.seismic.TraceHeaderFormat;
import cn.wisenergy.pai.hadoop2.seismic.TraceHeaderWritable;

public class ShuffledDataProcessor implements LogicalIOProcessor{
	private static final Log LOG=LogFactory.getLog(ShuffledDataProcessor.class);
	private TezCounters counters;
	TezProcessorContext context;
	@Override
	public void initialize(TezProcessorContext processorContext) throws Exception {
		context=processorContext;
		counters=context.getCounters();
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

		for(LogicalInput input:inputs.values()){
			KeyValuesReader reader =(KeyValuesReader)input.getReader();
			int i=0;
			while (reader.next()) {
				TraceHeaderWritable inKey = (TraceHeaderWritable) reader.getCurrentKey();
				Iterator it=reader.getCurrentValues().iterator();
				while(it.hasNext()){
					TraceDataWritable inValue = (TraceDataWritable) it.next();
					TraceHeaderWritable outKey =  new TraceHeaderWritable();
					TraceDataWritable outValue = new TraceDataWritable();
					doWorkflow(inKey, inValue, outKey, outValue);
					Iterator oit=outputs.values().iterator();
					while(oit.hasNext()){
						OnFileSortedOutput output = (OnFileSortedOutput)oit.next();
						KeyValueWriter kvWriter = (KeyValueWriter) output.getWriter();
						writeWithNewKeys(kvWriter,outKey,outValue);
					}
					i++;
					if(i%1000==0){
						CounterGroup cgroup=counters.getGroup("DataFlow");
						TezCounter counter=cgroup.findCounter(context.getTaskVertexName());
						counter.increment(1000);
					}
				}
				
			}
			CounterGroup cgroup=counters.getGroup("DataFlow");
			TezCounter counter=cgroup.findCounter(context.getTaskVertexName());
			counter.increment(i%1000);
			LOG.info("finished currentInput");
		}
		LOG.info("finished");
	}

	public void doWorkflow(TraceHeaderWritable inKey, TraceDataWritable inValue, TraceHeaderWritable outKey, TraceDataWritable outValue) {
		int[] keys = new int[inKey.getKeys().length];
		for(int i=0;i<keys.length;i++){
			keys[i]=inKey.getKeys()[i];
		}
		outKey.setKeys(keys);
		outKey.setHeaderBytes(inKey.getHeaderBytes());
		outValue.setDataBytes(inValue.getDataBytes());
	}
	private void writeWithNewKeys(KeyValueWriter writer,TraceHeaderWritable h,TraceDataWritable d) throws IOException{
		int[] keys = new int[3];
		SegyUtil util = new SegyUtil(h.getHeaderBytes(), false);
		keys[0] = util.readInt(TraceHeaderFormat.m_Offset);
		keys[1] = util.readInt(TraceHeaderFormat.m_InLineNo);
		keys[2] = util.readInt(TraceHeaderFormat.m_CrossLineNo);
		h.setKeys(keys);
		writer.write(h, d);
	}
}
