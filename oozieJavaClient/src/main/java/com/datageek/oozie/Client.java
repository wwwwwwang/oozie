package com.datageek.oozie;

import java.io.IOException;
import java.net.URL;

import com.datageek.oozie.utils.PropertiesReader;
import com.datageek.oozie.utils.Protocol;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.generic.GenericRequestor;
public class Client {
    private org.apache.avro.Protocol protocol = null;
    private String host = null;
    private int port = 0;
    private int count = 1;

    public Client(org.apache.avro.Protocol protocol, String host, int port, int count) {
        this.protocol = protocol;
        this.host = host;
        this.port = port;
        this.count = count;
    }

    public Client(org.apache.avro.Protocol protocol, String host, int port) {
        this.protocol = protocol;
        this.host = host;
        this.port = port;
    }

    public long sendMLParameters(String oozie, String path, String user, String add, String command, String name, String id) throws Exception {
        GenericRecord requestData = new GenericData.Record(protocol.getType("ml"));
        requestData.put("oozie", oozie);
        requestData.put("path", path);
        requestData.put("user", user);
        requestData.put("host",  add);
        requestData.put("command", command);
        requestData.put("name", name);
        requestData.put("id", id);

        GenericRecord request = new GenericData.Record(protocol.getMessages().get("action").getRequest());
        request.put("message", requestData);

        Transceiver t = new HttpTransceiver(new URL("http://" + host + ":" + port));
        GenericRequestor requestor = new GenericRequestor(protocol, t);

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Object result = requestor.request("action", request);
            if (result instanceof GenericData.Record) {
                GenericData.Record record = (GenericData.Record) result;
                System.out.println(record);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println((end - start)+"ms");
        return end - start;
    }

    public long sendBatchParameters(String oozie, String path, String user, String add, String command, int id) throws Exception {
        GenericRecord requestData = new GenericData.Record(protocol.getType("batch"));
        requestData.put("oozie", oozie);
        requestData.put("path", path);
        requestData.put("user", user);
        requestData.put("host",  add);
        requestData.put("command", command);
        requestData.put("batch_id", id);

        GenericRecord request = new GenericData.Record(protocol.getMessages().get("batch_req").getRequest());
        request.put("message", requestData);

        Transceiver t = new HttpTransceiver(new URL("http://" + host + ":" + port));
        GenericRequestor requestor = new GenericRequestor(protocol, t);

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Object result = requestor.request("batch_req", request);
            if (result instanceof GenericData.Record) {
                GenericData.Record record = (GenericData.Record) result;
                System.out.println(record);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println((end - start)+"ms");
        return end - start;
    }

    public long sendETLParameters(String oozie, String path, String user, String add, String command, String option) throws Exception {
        GenericRecord requestData = new GenericData.Record(protocol.getType("etl"));
        requestData.put("oozie", oozie);
        requestData.put("path", path);
        requestData.put("user", user);
        requestData.put("host",  add);
        requestData.put("command", command);
        requestData.put("name", option);

        GenericRecord request = new GenericData.Record(protocol.getMessages().get("etl").getRequest());
        request.put("message", requestData);

        Transceiver t = new HttpTransceiver(new URL("http://" + host + ":" + port));
        GenericRequestor requestor = new GenericRequestor(protocol, t);

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Object result = requestor.request("etl", request);
            if (result instanceof GenericData.Record) {
                GenericData.Record record = (GenericData.Record) result;
                System.out.println(record);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println((end - start)+"ms");
        return end - start;
    }

    public long sendKillParameters(String oozie, String path, String id, String nameNode, String jobTracker) throws Exception {
        GenericRecord requestData = new GenericData.Record(protocol.getType("kill"));
        requestData.put("oozie", oozie);
        requestData.put("path", path);
        requestData.put("nameNode", nameNode);
        requestData.put("jobTracker", jobTracker);
        requestData.put("id", id);

        GenericRecord request = new GenericData.Record(protocol.getMessages().get("kill").getRequest());
        request.put("message", requestData);

        Transceiver t = new HttpTransceiver(new URL("http://" + host + ":" + port));
        GenericRequestor requestor = new GenericRequestor(protocol, t);

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Object result = requestor.request("kill", request);
            if (result instanceof GenericData.Record) {
                GenericData.Record record = (GenericData.Record) result;
                System.out.println(record);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println((end - start)+"ms");
        return end - start;
    }

    /*public long run(String type, String oozie, String path, String user, String add, String command, int job_id, String id) {
        long res = 0;
        try {
            if(type.equalsIgnoreCase("ml")){
                res = sendMLParameters(oozie, path, user, add, command, job_id, id);
            }else if(type.equalsIgnoreCase("batch")){
                res = sendBatchParameters(oozie, path, user, add, command, job_id);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }*/

    public static void main(String[] args) throws Exception {

        //batch
        /*try {
            PropertiesReader.loadPropertiesByClassPath("conn.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String type = PropertiesReader.getProperty("oozie.batch.type");
        String ip = PropertiesReader.getProperty("oozie.batch.ip");
        int port = Integer.parseInt(PropertiesReader.getProperty("oozie.batch.port"));
        String oozie = PropertiesReader.getProperty("oozie.batch.oozie");
        String path = PropertiesReader.getProperty("oozie.batch.path");
        String user = PropertiesReader.getProperty("oozie.batch.user");
        String add = PropertiesReader.getProperty("oozie.batch.add");
        String command = PropertiesReader.getProperty("oozie.batch.command");
        *//*String type = "batch";
        String ip = "127.0.0.1";//args[0];
        int port = 9090;//Integer.parseInt(args[1]);*//*
        //String name = "Test";//args[0]
        int batch_id = 1;//Integer.parseInt(args[0]);
        new Client(Protocol.getProtocol(type+".avpr"), ip, port)
        .sendBatchParameters(oozie, path, user, add, command, batch_id);*/

        /*String type = "ml";
        String ip = "127.0.0.1";//args[0];
        int port = 9090;//Integer.parseInt(args[1]);
        String name = "GM";//args[2]
        String id = "14";//args[3]*/

        //machine learning
        /*try {
            PropertiesReader.loadPropertiesByClassPath("conn.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String type = PropertiesReader.getProperty("oozie.batch.type");
        String ip = PropertiesReader.getProperty("oozie.batch.ip");
        int port = Integer.parseInt(PropertiesReader.getProperty("oozie.batch.port"));
        String oozie = PropertiesReader.getProperty("oozie.batch.oozie");
        String path = PropertiesReader.getProperty("oozie.batch.path");
        String user = PropertiesReader.getProperty("oozie.batch.user");
        String add = PropertiesReader.getProperty("oozie.batch.add");
        String command = PropertiesReader.getProperty("oozie.batch.command");
        String name = "GM";//args[0]
        String id = "14";//args[1]

        new Client(Protocol.getProtocol(type+".avpr"), ip, port)
        .sendMLParameters(oozie, path, user, add, command, name, id);*/

        //yarn kill
        /*try {
            PropertiesReader.loadPropertiesByClassPath("conn.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String type = PropertiesReader.getProperty("oozie.kill.type");
        String ip = PropertiesReader.getProperty("oozie.kill.ip");
        int port = Integer.parseInt(PropertiesReader.getProperty("oozie.kill.port"));
        String oozie = PropertiesReader.getProperty("oozie.kill.oozie");
        String path = PropertiesReader.getProperty("oozie.kill.path");
        String nameNode = PropertiesReader.getProperty("oozie.kill.nameNode");
        String jobTracker = PropertiesReader.getProperty("oozie.kill.jobTracker");
        String id = "application_1483943698698_0481";//Integer.parseInt(args[0]);
        new Client(Protocol.getProtocol(type+".avpr"), ip, port)
                .sendKillParameters(oozie, path, id, nameNode, jobTracker);*/

        //etl
        try {
            PropertiesReader.loadPropertiesByClassPath("conn.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String type = PropertiesReader.getProperty("oozie.etl.type");
        String ip = PropertiesReader.getProperty("oozie.etl.ip");
        int port = Integer.parseInt(PropertiesReader.getProperty("oozie.etl.port"));
        String oozie = PropertiesReader.getProperty("oozie.etl.oozie");
        String path = PropertiesReader.getProperty("oozie.etl.path");
        String user = PropertiesReader.getProperty("oozie.etl.user");
        String add = PropertiesReader.getProperty("oozie.etl.add");
        String command = PropertiesReader.getProperty("oozie.etl.command");

        String option = "-k@2@engine@2@-b@2@172.31.18.14:9092,172.31.18.13:9092,172.31.18.12:9092@2@-t@2@bcm_engine@2@-f@2@bcm_engine@2@-i@2@10@2@-r";
        new Client(Protocol.getProtocol(type+".avpr"), ip, port)
        .sendETLParameters(oozie, path, user, add, command, option);

    }
}
