package com.datageek.oozie;

import com.datageek.oozie.utils.MysqlJDBCDao;
import com.datageek.oozie.utils.PropertiesReader;
import org.apache.log4j.Logger;
import org.apache.oozie.client.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class OozieJavaClient4Coordinator {
    private String oozie;
    private String nameNode;
    private String jobTracker;
    private String queueName;
    private MysqlJDBCDao myjdbc;
    private static Logger log = Logger.getLogger(OozieJavaClient4Coordinator.class);

    public OozieJavaClient4Coordinator() {
        try {
            PropertiesReader.loadPropertiesByClassPath("conn.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        oozie = PropertiesReader.getProperty("oozie");
        nameNode = PropertiesReader.getProperty("nameNode");
        jobTracker = PropertiesReader.getProperty("jobTracker");
        queueName = PropertiesReader.getProperty("queueName");
        myjdbc = new MysqlJDBCDao();

        log.info("oozie = "+ oozie);
        log.info("nameNode = "+ nameNode);
        log.info("jobTracker = "+ jobTracker);
        log.info("queueName = "+ queueName);
    }


    public void submitWorkFlow4Coordinator(String path, String flowpath, String start, String end, String frequency,
                                           String user, String ipadd, String command, String id) {
        OozieClient wc = new OozieClient(oozie);
        Properties conf = wc.createConfiguration();
        conf.setProperty(OozieClient.COORDINATOR_APP_PATH, path);
        //conf.setProperty("workflowAppUri", "hdfs://nameservice1/user/oozie/examples/apps/sshWithParameters1");
        conf.setProperty("workflowAppUri", flowpath);
        conf.setProperty("nameNode", nameNode);
        conf.setProperty("jobTracker", jobTracker);
        conf.setProperty("queueName", queueName);
        conf.setProperty("start", start);
        conf.setProperty("end", end);
        conf.setProperty("frequency", frequency);
        conf.setProperty("user", user);
        conf.setProperty("ipadd", ipadd);
        conf.setProperty("command", command);
        conf.setProperty("name", id);
        String jobId = "";
        try {
            jobId = wc.run(conf);
        } catch (OozieClientException e) {
            e.printStackTrace();
        }
        log.info("Workflow job submitted, jod id =" + jobId);
        System.out.println("Workflow job submitted, jod id =" + jobId);
        if(!"".equalsIgnoreCase(jobId.trim())){
            String timestamp = String.valueOf(System.currentTimeMillis());
            myjdbc.save(id, jobId, "coordinatorJob"+id, timestamp);
        }
        /*try {
            log.info(wc.getCoordJobInfo(jobId));
            System.out.println(wc.getCoordJobInfo(jobId));
        } catch (OozieClientException e) {
            e.printStackTrace();
        }*/
        try {
            Thread.sleep(3 * 1000);
            while (wc.getCoordJobInfo(jobId).getStatus() == CoordinatorJob.Status.RUNNING
                    || wc.getCoordJobInfo(jobId).getStatus() == CoordinatorJob.Status.PREP) {
                System.out.println("job status: " + wc.getJobInfo(jobId).getStatus());
                System.out.println("==========="+wc.getCoordJobInfo(jobId));
                List<CoordinatorAction> actions = wc.getCoordJobInfo(jobId).getActions();
                for(int i=0; i<actions.size(); i++){
                    System.out.println("*****************************i="+i+"*******************");
                    System.out.println("===========jobid="+actions.get(i).getJobId());
                    System.out.println("===========id="+actions.get(i).getId());
                    System.out.println("===========externalid="+actions.get(i).getExternalId());
                    System.out.println("===========status="+actions.get(i).getStatus());
                    System.out.println("===========actionnumber="+actions.get(i).getActionNumber());
                    System.out.println("===========createtime="+actions.get(i).getCreatedTime());
                    System.out.println("===========nominaltime="+actions.get(i).getNominalTime());
                    System.out.println("*****************************end*******************");
                }
                //System.out.println("==========="+wc.getCoordJobInfo(jobId).getActions());
                //System.out.println("==========="+wc.getCoordActionInfo());
                Thread.sleep(10 * 1000);
            }
            System.out.println("Workflow job completed ...");
            System.out.println(wc.getJobInfo(jobId));
        } catch (OozieClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void kill(String appid){
        OozieClient wc = new OozieClient(oozie);
        try {
            System.out.println("jobid " + appid + " will be killed..");
            wc.kill(appid);
            System.out.println("kill finished ..");
        } catch (OozieClientException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        OozieJavaClient4Coordinator oozieclient = new OozieJavaClient4Coordinator();
        String start = "2017-02-17T08:00+0800";
        String end = "2017-02-17T09:40+0800";
        String frequency = "0/30 * * * *";
        String workflowAppUri = "hdfs://nameservice1/user/oozie/examples/apps/cron-whsh";
        try {
            PropertiesReader.loadPropertiesByClassPath("conn.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String path = PropertiesReader.getProperty("oozie.batch.path");
        String user = PropertiesReader.getProperty("oozie.batch.user");
        String add = PropertiesReader.getProperty("oozie.batch.add");
        String command = PropertiesReader.getProperty("oozie.batch.command");
        String id = "1";

        oozieclient.submitWorkFlow4Coordinator(workflowAppUri, path, start, end, frequency, user, add, command, id);


        /*String jobid = "0000121-170214123146983-oozie-oozi-C";
        OozieJavaClient4Coordinator oozieclient = new OozieJavaClient4Coordinator();

        oozieclient.kill(jobid);*/

    }
}
