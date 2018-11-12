package com.datageek.oozie;

import com.datageek.oozie.utils.MysqlJDBCDao;
import com.datageek.oozie.utils.PropertiesReader;
import org.apache.log4j.Logger;
import org.apache.oozie.client.*;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class OozieJavaClient2 {
    private String oozie;
    private String nameNode;
    private String jobTracker;
    private String queueName;
    private MysqlJDBCDao myjdbc;
    private static Logger log = Logger.getLogger(OozieJavaClient2.class);

    public OozieJavaClient2() {
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

        log.info("oozie = " + oozie);
        log.info("nameNode = " + nameNode);
        log.info("jobTracker = " + jobTracker);
        log.info("queueName = " + queueName);
    }

    public void submitWorkFlow(String path, String user, String add, String command) {
        submitWorkFlow(path, user, add, command, "", "");
    }

    public void submitWorkFlow(String path, String user, String add, String command, String name) {
        submitWorkFlow(path, user, add, command, name, "");
    }

    public void submitWorkFlow(String path, String user, String add, String command, String name, String id) {
        OozieClient wc = new OozieClient(oozie);
        Properties conf = wc.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, path);
        conf.setProperty("user", user);
        conf.setProperty("ipadd", add);
        conf.setProperty("command", command);
        if (!name.trim().equalsIgnoreCase("")) {
            conf.setProperty("name", name);
        }
        if (!id.trim().equalsIgnoreCase("")) {
            conf.setProperty("id", id);
        }
        String jobId = "";
        try {
            jobId = wc.run(conf);
        } catch (OozieClientException e) {
            e.printStackTrace();
        }
        System.out.println("Workflow job submitted, jod id =" + jobId);
        // wait until the workflow job finishes printing the status every 10 seconds
        try {
            while (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
                System.out.println("Workflow job running ...");
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

    public void submitWorkFlow4Kill(String path, String id) {
        OozieClient wc = new OozieClient(oozie);
        Properties conf = wc.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, path);
        conf.setProperty("nameNode", nameNode);
        conf.setProperty("jobTracker", jobTracker);
        if (!id.trim().equalsIgnoreCase("")) {
            conf.setProperty("id", id);
        }
        String jobId = "";
        try {
            jobId = wc.run(conf);
        } catch (OozieClientException e) {
            e.printStackTrace();
        }
        System.out.println("Workflow job submitted, jod id =" + jobId);
        // wait until the workflow job finishes printing the status every 10 seconds
        try {
            int cnt = 1;
            while (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
                System.out.println("Workflow job of killing is running ...");
                Thread.sleep(10 * 1000);
                cnt++;
                if (cnt >= 18) {
                    System.out.println("Workflow job runs too long time, no showing any more...");
                    break;
                }
            }
            System.out.println("Workflow job of killing is completed ...");
            System.out.println(wc.getJobInfo(jobId));
        } catch (OozieClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void submitWorkFlow4Coordinator(String path, String flowpath, String start, String end, String frequency,
                                           String user, String add, String command, String name, String id) {
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
        conf.setProperty("ipadd", add);
        conf.setProperty("command", command);
        conf.setProperty("name", name);
        if (!id.trim().equalsIgnoreCase("")) {
            conf.setProperty("id", id);
        }
        String jobId = "";
        try {
            jobId = wc.run(conf);
        } catch (OozieClientException e) {
            e.printStackTrace();
        }
        log.info("Workflow job submitted, jod id =" + jobId);
        System.out.println("Workflow job submitted, jod id =" + jobId);
        if (!"".equalsIgnoreCase(jobId.trim())) {
            String timestamp = String.valueOf(System.currentTimeMillis());
            myjdbc.save(name, jobId, "coordinatorJob" + name, timestamp);
        }
        try {
            String timestamp = String.valueOf(System.currentTimeMillis());
            myjdbc.saveStatus(name, wc.getJobInfo(jobId).getStatus().toString(), "coordinatorJob" + name, timestamp);
            Thread.sleep(1000);
            timestamp = String.valueOf(System.currentTimeMillis());
            myjdbc.saveStatus(name, wc.getJobInfo(jobId).getStatus().toString(), "coordinatorJob" + name, timestamp);
            Thread.sleep(1000);
            timestamp = String.valueOf(System.currentTimeMillis());
            myjdbc.saveStatus(name, wc.getJobInfo(jobId).getStatus().toString(), "coordinatorJob" + name, timestamp);
            Thread.sleep(1000);
            int running = 0;
            boolean isRunning = false;
            while (wc.getCoordJobInfo(jobId).getStatus() == CoordinatorJob.Status.RUNNING
                    || wc.getCoordJobInfo(jobId).getStatus() == CoordinatorJob.Status.PREP) {
                //System.out.println("job status: " + wc.getJobInfo(jobId).getStatus());
                timestamp = String.valueOf(System.currentTimeMillis());
                myjdbc.saveStatus(name, wc.getJobInfo(jobId).getStatus().toString(), "coordinatorJob" + name, timestamp);
                //System.out.println("===========" + wc.getCoordJobInfo(jobId));
                List<CoordinatorAction> actions = wc.getCoordJobInfo(jobId).getActions();
                if(running < actions.size()){
                    if (!isRunning && actions.get(running).getStatus().toString().equalsIgnoreCase("running")) {
                        myjdbc.saveSubStatus(name, actions.get(running).getJobId(), actions.get(running).getId(), actions.get(running).getExternalId(),
                                actions.get(running).getStatus().toString(), String.valueOf(actions.get(running).getActionNumber()),
                                actions.get(running).getCreatedTime().toString(), actions.get(running).getNominalTime().toString(), timestamp);
                        isRunning = true;
                        System.out.println("===========action " + running + " starts.....");
                    } else if (isRunning &&
                            !actions.get(running).getStatus().toString().equalsIgnoreCase("running")) {
                        myjdbc.saveSubStatus(name, actions.get(running).getJobId(), actions.get(running).getId(), actions.get(running).getExternalId(),
                                actions.get(running).getStatus().toString(), String.valueOf(actions.get(running).getActionNumber()),
                                actions.get(running).getCreatedTime().toString(), actions.get(running).getNominalTime().toString(), timestamp);
                        isRunning = false;
                        System.out.println("===========action " + running + " ends.....");
                        running++;
                    } else {
                        System.out.println("===========running= " + running
                                + ", status = " + actions.get(running).getStatus());
                    }
                }
                Thread.sleep(10 * 1000);
            }
            System.out.println("Workflow job completed ...");
            timestamp = String.valueOf(System.currentTimeMillis());
            myjdbc.saveStatus(name, wc.getJobInfo(jobId).getStatus().toString(), "coordinatorJob" + name, timestamp);
            System.out.println(wc.getJobInfo(jobId));
        } catch (OozieClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void kill(String appid) {
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
        OozieJavaClient2 oozieclient = new OozieJavaClient2();
        //String oozie = "http://172.31.18.13:11000/oozie";
        String path = "hdfs://nameservice1/user/oozie/examples/apps/sshWithParameters";
        String user = "oozie";
        String add = "hadoop103.dategeek.com.cn";
        String command = "/var/lib/oozie/newtest.sh";
        oozieclient.submitWorkFlow(path, user, add, command);
    }
}
