package cn.wisenergy.pai.tez;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.split.TezGroupedSplitsInputFormat;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.mapreduce.committer.MROutputCommitter;
import org.apache.tez.mapreduce.common.MRInputAMSplitGenerator;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.api.TezRootInputInitializer;
import org.apache.tez.runtime.library.input.ShuffledMergedInput;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;

import cn.wisenergy.pai.hadoop2.seismic.TraceDataWritable;
import cn.wisenergy.pai.hadoop2.seismic.TraceHeaderWritable;
import cn.wisenergy.pai.tez.format.DataFlowInputFormat;
import cn.wisenergy.pai.tez.format.DataFlowOutputFormat;
import cn.wisenergy.pai.tez.format.TraceHeaderPartitioner;
import cn.wisenergy.pai.tez.processor.InputProcessor;
import cn.wisenergy.pai.tez.processor.OutputProcessor;
import cn.wisenergy.pai.tez.processor.ShuffledDataProcessor;

public class DAGCreator {
	
	private final static Log LOG=LogFactory.getLog(DAGCreator.class);
	
	public static DAG createDAG(Configuration conf, String inputPath, String outputPath) throws IOException {
		
		Configuration shotConf = new Configuration(conf);
		shotConf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS, TraceHeaderWritable.class.getName());
		shotConf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS, TraceDataWritable.class.getName());
		shotConf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, 
		        TezGroupedSplitsInputFormat.class.getName());
		shotConf.set(DataFlowInputFormat.INPUT_DATAS_PATH, inputPath);
		shotConf.setClass(MRJobConfig.PARTITIONER_CLASS_ATTR, TraceHeaderPartitioner.class,Partitioner.class);
		shotConf.setBoolean("mapred.mapper.new-api", true);
		MultiStageMRConfToTezTranslator.translateVertexConfToTez(shotConf, null);
		
		Configuration cmpConf = new Configuration((Configuration) conf);
		cmpConf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS, TraceHeaderWritable.class.getName());
		cmpConf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS, TraceDataWritable.class.getName());
		cmpConf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, DataFlowOutputFormat.class,OutputFormat.class);
		cmpConf.set(FileOutputFormat.OUTDIR, outputPath);
		cmpConf.setBoolean("mapred.mapper.new-api", true);
		MultiStageMRConfToTezTranslator.translateVertexConfToTez(cmpConf, shotConf);
		
		
		Configuration offsetConf = new Configuration((Configuration) conf);
		offsetConf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS, TraceHeaderWritable.class.getName());
		offsetConf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS, TraceDataWritable.class.getName());
		offsetConf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, DataFlowOutputFormat.class,OutputFormat.class);
		offsetConf.set(FileOutputFormat.OUTDIR, outputPath);
		offsetConf.setBoolean("mapred.mapper.new-api", true);
		MultiStageMRConfToTezTranslator.translateVertexConfToTez(offsetConf, cmpConf);
		
		
		MRHelpers.doJobClientMagic(shotConf);
		MRHelpers.doJobClientMagic(cmpConf);
		MRHelpers.doJobClientMagic(offsetConf);
		
		
		//about input 
		byte[] shotPayload = MRHelpers.createUserPayloadFromConf(shotConf);
		byte[] shotPayloadwithInputFormat = MRHelpers.createMRInputPayloadWithGrouping(shotPayload, DataFlowInputFormat.class.getName());
		LOG.info("the mapInputPayload length "+shotPayloadwithInputFormat.length);
		Vertex shotVertex = new Vertex("shot", new ProcessorDescriptor(InputProcessor.class.getName()).setUserPayload(shotPayloadwithInputFormat), -1, MRHelpers.getMapResource(shotConf));
		shotVertex.setJavaOpts(MRHelpers.getMapJavaOpts(shotConf));
		Map<String, String> mapEnv = new HashMap<String, String>();
		MRHelpers.updateEnvironmentForMRTasks(shotConf, mapEnv, true);
		shotVertex.setTaskEnvironment(mapEnv);
		Class<? extends TezRootInputInitializer> initializerClazz = MRInputAMSplitGenerator.class;
		InputDescriptor id = new InputDescriptor(MRInput.class.getName()).setUserPayload(shotPayloadwithInputFormat);
		shotVertex.addInput("MRInput", id, initializerClazz);
		
		
		
		//middle processors
		byte[] cmpPayload = MRHelpers.createUserPayloadFromConf(cmpConf);
		Vertex cmpVertex = new Vertex("cmp", new ProcessorDescriptor(ShuffledDataProcessor.class.getName()).setUserPayload(cmpPayload), 10, MRHelpers.getReduceResource(cmpConf));
		cmpVertex.setJavaOpts(MRHelpers.getReduceJavaOpts(cmpConf));
		Map<String,String> cmpEnv=new HashMap<String,String>();
		MRHelpers.updateEnvironmentForMRTasks(cmpConf,cmpEnv, false);
		cmpVertex.setTaskEnvironment(cmpEnv);
		
		//about output 
		byte[] offsetPayload = MRHelpers.createUserPayloadFromConf(offsetConf);
		Vertex offsetVertex = new Vertex("offset", new ProcessorDescriptor(OutputProcessor.class.getName()).setUserPayload(offsetPayload), 10, MRHelpers.getReduceResource(offsetConf));
		offsetVertex.setJavaOpts(MRHelpers.getReduceJavaOpts(offsetConf));
		Map<String,String> offsetEnv=new HashMap<String,String>();
		MRHelpers.updateEnvironmentForMRTasks(offsetConf,offsetEnv, false);
		offsetVertex.setTaskEnvironment(offsetEnv);
		OutputDescriptor od = new OutputDescriptor(MROutput.class.getName()).setUserPayload(offsetPayload);
		offsetVertex.addOutput("MROutput", od, MROutputCommitter.class);
		
		
		Edge edge1 = new Edge(shotVertex, cmpVertex, new EdgeProperty(DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, new OutputDescriptor(
				OnFileSortedOutput.class.getName()).setUserPayload(shotPayload), new InputDescriptor(ShuffledMergedInput.class.getName()).setUserPayload(cmpPayload)));
		
		Edge edge2 = new Edge(cmpVertex, offsetVertex, new EdgeProperty(DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, new OutputDescriptor(
				OnFileSortedOutput.class.getName()).setUserPayload(cmpPayload), new InputDescriptor(ShuffledMergedInput.class.getName()).setUserPayload(offsetPayload)));
		
		
		DAG dag = new DAG("DataFlow");
		dag.addVertex(shotVertex).addVertex(cmpVertex).addVertex(offsetVertex).addEdge(edge1).addEdge(edge2);
		return dag;
	}

}
