package com.datageek.oozie;

import com.datageek.oozie.utils.PropertiesReader;
import com.datageek.oozie.utils.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.generic.GenericResponder;

import java.io.IOException;
import java.util.Properties;

public class Server2 extends GenericResponder {

    private org.apache.avro.Protocol protocol = null;
    private int port;
    private Properties props = null;

    public Server2(org.apache.avro.Protocol protocol, int port) {
        super(protocol);
        this.protocol = protocol;
        this.port = port;
        try {
            props = PropertiesReader.loadPropertiesByClassPath("conn.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public Object respond(Message message, Object request) throws Exception {
        String path = "";
        String user = "";
        String host = "";
        String command = "";
        OozieJavaClient2 oozieclient = new OozieJavaClient2();
        GenericRecord req = (GenericRecord) request;
        GenericRecord reMessage = new GenericData.Record(protocol.getType("responseType"));
        if (message.getName().equals("datageek")) {
            GenericRecord msg = (GenericRecord) req.get("message");
            String type = msg.get("type").toString();
            System.out.print("*************接收" + type + "类型任务参数， ");
            if (type.equalsIgnoreCase("batch")) {
                System.out.println("参数详情：");
                System.out.println(msg.get("arg1"));
                path = props.get("oozie.batch.path").toString();
                user = props.get("oozie.batch.user").toString();
                host = props.get("oozie.batch.add").toString();
                command = props.get("oozie.batch.command").toString();
                reMessage.put("responseName", "The group of Batch job parameters are received!! ");
                oozieclient.submitWorkFlow(path, user, host, command, msg.get("arg1").toString());
            } else if (type.equalsIgnoreCase("ml")) {
                System.out.println("参数详情：");
                System.out.println(msg.get("arg1") + " " + msg.get("arg2"));
                path = props.get("oozie.ml.path").toString();
                user = props.get("oozie.ml.user").toString();
                host = props.get("oozie.ml.add").toString();
                command = props.get("oozie.ml.command").toString();
                reMessage.put("responseName", "The group of ML parameters are received!! ");
                oozieclient.submitWorkFlow(path, user, host, command,
                        msg.get("arg1").toString(), msg.get("arg2").toString());
            } else if (type.equalsIgnoreCase("mlPredict")) {
                System.out.println("参数详情：");
                System.out.println(msg.get("arg1"));
                path = props.get("oozie.mlPredict.path").toString();
                user = props.get("oozie.mlPredict.user").toString();
                host = props.get("oozie.mlPredict.add").toString();
                command = props.get("oozie.mlPredict.command").toString();
                reMessage.put("responseName", "The group of mlPredict job parameters are received!! ");
                oozieclient.submitWorkFlow(path, user, host, command, msg.get("arg1").toString());
            } else if (type.equalsIgnoreCase("kill")) {
                System.out.println("参数详情：");
                System.out.println(msg.get("arg1"));
                path = props.get("oozie.kill.path").toString();
                reMessage.put("responseName", "The group of yarn killing parameters are received!! ");
                oozieclient.submitWorkFlow4Kill(path, msg.get("arg1").toString());
            } else if (type.equalsIgnoreCase("etl")) {
                System.out.println("参数详情：");
                System.out.println(msg.get("arg1"));
                path = props.get("oozie.etl.path").toString();
                user = props.get("oozie.etl.user").toString();
                host = props.get("oozie.etl.add").toString();
                command = props.get("oozie.etl.command").toString();
                reMessage.put("responseName", "The group of etlWithKafka job parameters are received!! ");
                oozieclient.submitWorkFlow(path, user, host, command,
                        msg.get("arg1").toString());
            } else if (type.equalsIgnoreCase("coordinatorkill")) {
                System.out.println("参数详情：");
                System.out.println(msg.get("arg1"));
                reMessage.put("responseName", "The group of coordinator kill job parameters are received!! ");
                oozieclient.kill(msg.get("arg1").toString());
            } else if (type.equalsIgnoreCase("coordinator")) {
                System.out.println("参数详情：");
                System.out.println(msg.get("start") + " " + msg.get("end") + " " + msg.get("frequency")
                        + " " + msg.get("subtype") + " " + msg.get("arg1") + " " + msg.get("arg2"));
                String coPath = props.get("oozie.coordinator.workflowAppUri").toString();
                if (msg.get("subtype").toString().equalsIgnoreCase("batch")) {
                    path = props.get("oozie.batch.path").toString();
                    user = props.get("oozie.batch.user").toString();
                    host = props.get("oozie.batch.add").toString();
                    command = props.get("oozie.batch.command").toString();
                } else if (msg.get("subtype").toString().equalsIgnoreCase("ml")) {
                    path = props.get("oozie.ml.path").toString();
                    user = props.get("oozie.ml.user").toString();
                    host = props.get("oozie.ml.add").toString();
                    command = props.get("oozie.ml.command").toString();
                } else if (msg.get("subtype").toString().equalsIgnoreCase("mlPredict")) {
                    path = props.get("oozie.mlPredict.path").toString();
                    user = props.get("oozie.mlPredict.user").toString();
                    host = props.get("oozie.mlPredict.add").toString();
                    command = props.get("oozie.mlPredict.command").toString();
                }
                reMessage.put("responseName", "The group of coordinator job parameters are received!! ");
                oozieclient.submitWorkFlow4Coordinator(coPath, path, msg.get("start").toString(),
                        msg.get("end").toString(), msg.get("frequency").toString(), user, host, command,
                        msg.get("arg1").toString(), msg.get("arg2").toString());
            }
        }

        return reMessage;
    }


    public void run() {
        try {
            HttpServer server = new HttpServer(this, port);
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //String protocol = args[0];
        int port = 9090;
        port = Integer.parseInt(args[0]);
        new Server2(Protocol.getProtocol("datageek.avpr"), port).run();
        //new Server(Protocol.getProtocol("ml.avpr"), 9090).run();
    }
}
