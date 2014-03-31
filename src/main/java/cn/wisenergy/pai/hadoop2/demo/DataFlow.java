package cn.wisenergy.pai.hadoop2.demo;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.EnumSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.tez.client.TezSession;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;

import cn.wisenergy.pai.hadoop2.seismic.TraceHeader;
import cn.wisenergy.pai.tez.DAGCreator;
import cn.wisenergy.pai.tez.SessionCreator;

public class DataFlow extends Configured implements Tool {
	
	private static final Log LOG=LogFactory.getLog(DataFlow.class);
	private static final DecimalFormat formatter = new DecimalFormat("###.##%");

	/**
	 * v1->v2->v3
	 */
	@Override
	public int run(String[] args) throws Exception {
		
		String input="/";
		String output="/";
		boolean isPrintHeader=false;
		for (int i = 0; i < args.length; i++) { // parse command line
			if (args[i].startsWith("-input")) {
				input=args[++i];
			} else if (args[i].equals("-output")) {
				output=args[++i];
			} else if (args[i].equals("printHeader")){
				isPrintHeader=true;
			}
		}
		LOG.info("input :"+input);
		
		if(isPrintHeader){
			readTraceHeader(input+".info");
		}else{
			
			TezSession session = null;
			
			Configuration conf = new Configuration();
			conf.setBoolean(ShuffleVertexManager.TEZ_AM_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL,false);
			LOG.info("create tez session.");
			session=SessionCreator.getSession(conf);
			
			LOG.info("output :"+output);
			try {
			LOG.info("create dag job.");
			DAG dag=DAGCreator.createDAG(conf,input,output);
			LOG.info("verify dag job.");
			dag.verify();
			LOG.info("submit dag job.");
	        DAGClient dagClient = session.submitDAG(dag);
	        LOG.info("finished dag submit.");
	        LOG.info("the aplication id is "+dagClient.getApplicationId());
	        
	        

	        DAGStatus dagStatus = null;
	        String[] vNames = { "shot", "cmp","offset" };

	        Set<StatusGetOpts> statusGetOpts = EnumSet.of(StatusGetOpts.GET_COUNTERS);
	        
	   
	            //dagClient = tezClient.submitDAGApplication(dag, amConfig);

	            // monitoring
	            while (true) {
	              dagStatus = dagClient.getDAGStatus(statusGetOpts);
	              if(dagStatus.getState() == DAGStatus.State.RUNNING ||
	                  dagStatus.getState() == DAGStatus.State.SUCCEEDED ||
	                  dagStatus.getState() == DAGStatus.State.FAILED ||
	                  dagStatus.getState() == DAGStatus.State.KILLED ||
	                  dagStatus.getState() == DAGStatus.State.ERROR) {
	                break;
	              }
	              try {
	                Thread.sleep(500);
	              } catch (InterruptedException e) {
	                // continue;
	              }
	            }


	            while (dagStatus.getState() == DAGStatus.State.RUNNING) {
	              try {
	                printDAGStatus(dagClient, vNames);
	                try {
	                  Thread.sleep(1000);
	                } catch (InterruptedException e) {
	                  // continue;
	                }
	                dagStatus = dagClient.getDAGStatus(statusGetOpts);
	              } catch (TezException e) {
	                System.exit(-1);
	              }
	            }
	            printDAGStatus(dagClient, vNames,
	                true, true);
	            System.out.println("DAG completed. " + "FinalState=" + dagStatus.getState());
	            if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
	              System.out.println("DAG diagnostics: " + dagStatus.getDiagnostics());
	            }
	        } finally {
	        	if(session != null){
	        		session.stop();
	        	}
	        }
		}
		
        
		return 0;
	}
	private void readTraceHeader(String headerPath) throws IllegalArgumentException, IOException{
		FileSystem fs=FileSystem.get(new Configuration());
		FileStatus s=fs.getFileStatus(new Path(headerPath));
		LOG.info("file length is "+s.getLen());
		FSDataInputStream in=fs.open(new Path(headerPath), 4096);
		byte[] buffer=new byte[1024];
		TraceHeader th=new TraceHeader();
		int i=0;
		while(in.read(buffer)>0){
			th.setData(buffer);
			System.out.println(th.toString());
			i++;
			if(i==100){
				break;
			}
		}
		in.close();
		
	}
	  public static void printDAGStatus(DAGClient dagClient, String[] vertexNames,
		      boolean displayDAGCounters, boolean displayVertexCounters)
		      throws IOException, TezException {
		    Set<StatusGetOpts> opts = EnumSet.of(StatusGetOpts.GET_COUNTERS);
		    DAGStatus dagStatus = dagClient.getDAGStatus(
		      (displayDAGCounters ? opts : null));
		    Progress progress = dagStatus.getDAGProgress();
		    double vProgressFloat = 0.0f;
		    if (progress != null) {
		      System.out.println("");
		      LOG.info("DAG: State: "
		          + dagStatus.getState()
		          + " Progress: "
		          + (progress.getTotalTaskCount() < 0 ? formatter.format(0.0f) :
		            formatter.format((double)(progress.getSucceededTaskCount())
		              /progress.getTotalTaskCount())));
		      for (String vertexName : vertexNames) {
		        VertexStatus vStatus = dagClient.getVertexStatus(vertexName,
		          (displayVertexCounters ? opts : null));
		        if (vStatus == null) {
		        	LOG.info("Could not retrieve status for vertex: "
		            + vertexName);
		          continue;
		        }
		        Progress vProgress = vStatus.getProgress();
		        if (vProgress != null) {
		          vProgressFloat = 0.0f;
		          if (vProgress.getTotalTaskCount() == 0) {
		            vProgressFloat = 1.0f;
		          } else if (vProgress.getTotalTaskCount() > 0) {
		            vProgressFloat = (double)vProgress.getSucceededTaskCount()
		              /vProgress.getTotalTaskCount();
		          }
		          LOG.info("VertexStatus:"
		              + " VertexName: "
		              + vertexName
		              + " Progress: " + formatter.format(vProgressFloat));
		        }
		        if (displayVertexCounters) {
		          TezCounters counters = vStatus.getVertexCounters();
		          if (counters != null) {
		            System.out.println("Vertex Counters for " + vertexName + ": "
		              + counters);
		          }
		        }
		      }
		    }
		    if (displayDAGCounters) {
		      TezCounters counters = dagStatus.getDAGCounters();
		      if (counters != null) {
		        System.out.println("DAG Counters: " + counters);
		      }
		    }
		  }
	  public static void printDAGStatus(DAGClient dagClient, String[] vertexNames)
		      throws IOException, TezException {
		    printDAGStatus(dagClient, vertexNames, false, false);
		  }

}
