package com.datageek.oozie;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

import java.util.Properties;

public class OozieJavaClient {
    public void submitWorkFlow(String oozie, String path, String user, String add, String command) {
        submitWorkFlow(oozie, path, user, add, command, "", "");
    }

    public void submitWorkFlow(String oozie, String path, String user, String add, String command, String name) {
        submitWorkFlow(oozie, path, user, add, command, name, "");
    }

    public void submitWorkFlow(String oozie, String path, String user, String add, String command, int id) {
        submitWorkFlow(oozie, path, user, add, command, String.valueOf(id), "");
    }

    public void submitWorkFlow(String oozie, String path, String user, String add, String command, String name, String id) {
        OozieClient wc = new OozieClient(oozie);
        Properties conf = wc.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, path);
        conf.setProperty("user", user);
        conf.setProperty("ipadd", add);
        conf.setProperty("command", command);
        if(!name.trim().equalsIgnoreCase("")) {
            conf.setProperty("name", name);
        }
        if(!id.trim().equalsIgnoreCase("")){
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

    public void submitWorkFlow4Kill(String oozie, String path, String id, String nameNode, String jobTracker) {
        OozieClient wc = new OozieClient(oozie);
        Properties conf = wc.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, path);
        conf.setProperty("nameNode", nameNode);
        conf.setProperty("jobTracker", jobTracker);
        if(!id.trim().equalsIgnoreCase("")){
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
                if(cnt>=18){
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

    public static void main(String[] args) {
        OozieJavaClient oozieclient =new OozieJavaClient();
        String oozie = "http://172.31.18.13:11000/oozie";
        String path = "hdfs://nameservice1/user/oozie/examples/apps/sshWithParameters";
        String user = "oozie";
        String add = "hadoop103.dategeek.com.cn";
        String command = "/var/lib/oozie/newtest.sh";
        oozieclient.submitWorkFlow(oozie, path, user, add, command);
    }
}
