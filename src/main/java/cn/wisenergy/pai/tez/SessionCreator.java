package cn.wisenergy.pai.tez;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.AMConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.client.TezSession;
import org.apache.tez.client.TezSessionConfiguration;
import org.apache.tez.client.TezSessionStatus;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;

public class SessionCreator {
	
	
	public static TezSession getSession(Configuration conf) throws IOException, TezException{
		TezConfiguration tezConf = new TezConfiguration(conf);
		return getSession(tezConf);
	}
	public static TezSession getSession(TezConfiguration tezConf) throws IOException, TezException{
		TezClient client = new TezClient(tezConf);
		ApplicationId appId = client.createApplication();
		UserGroupInformation.setConfiguration(tezConf);
		String user = UserGroupInformation.getCurrentUser().getShortUserName();
		// staging dir
		FileSystem fs = FileSystem.get(tezConf);
		String stagingDirStr = Path.SEPARATOR + "user" + Path.SEPARATOR + user + Path.SEPARATOR + ".staging"
				+ Path.SEPARATOR + Path.SEPARATOR + appId.toString();
		Path stagingDir = new Path(stagingDirStr);
		tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirStr);
		stagingDir = fs.makeQualified(stagingDir);
		TezClientUtils.ensureStagingDirExists(tezConf, stagingDir);
		TezSession tezSession = null;
		AMConfiguration amConfig = new AMConfiguration(null, null, tezConf, null);

		TezSessionConfiguration sessionConfig = new TezSessionConfiguration(amConfig, tezConf);
		tezSession = new TezSession("TezDataFlow", appId, sessionConfig);
		tezSession.start();
		waitForTezSessionReady(tezSession);
		return tezSession;
	}
	private static void waitForTezSessionReady(TezSession tezSession) throws IOException, TezException {
		while (true) {
			TezSessionStatus status = tezSession.getSessionStatus();
			if (status.equals(TezSessionStatus.SHUTDOWN)) {
				throw new RuntimeException("TezSession has already shutdown");
			}
			if (status.equals(TezSessionStatus.READY)) {
				return;
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				return;
			}
		}
	}
}
