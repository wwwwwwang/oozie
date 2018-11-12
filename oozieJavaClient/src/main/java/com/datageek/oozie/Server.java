package com.datageek.oozie;

import com.datageek.oozie.utils.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.generic.GenericResponder;

public class Server extends GenericResponder {

    private org.apache.avro.Protocol protocol = null;
    private int port;

    public Server(org.apache.avro.Protocol protocol, int port) {
        super(protocol);
        this.protocol = protocol;
        this.port = port;
    }

    @Override
    public Object respond(Message message, Object request) throws Exception {
        OozieJavaClient oozieclient = new OozieJavaClient();
        GenericRecord req = (GenericRecord) request;
        GenericRecord reMessage = null;
        if (message.getName().equals("action")) {
            GenericRecord msg = (GenericRecord) req.get("message");
            System.out.print("接收到数据：");
            System.out.println(msg.get("oozie") + " " + msg.get("path") + " " + msg.get("user")
                    + " " + msg.get("host") + " " + msg.get("command") + " " + msg.get("name")
                    + " " + msg.get("id"));
            //取得返回值的类型
            reMessage = new GenericData.Record(protocol.getType("responseType"));
            reMessage.put("responseName", "The group of ML　parameters are received!! ");
            oozieclient.submitWorkFlow(msg.get("oozie").toString(), msg.get("path").toString(),
                    msg.get("user").toString(), msg.get("host").toString(),
                    msg.get("command").toString(), msg.get("name").toString(),
                    msg.get("id").toString());
        } else if (message.getName().equals("batch_req")) {
            GenericRecord msg = (GenericRecord) req.get("message");
            System.out.print("接收到数据：");
            System.out.println(msg.get("oozie") + " " + msg.get("path") + " " + msg.get("user")
                    + " " + msg.get("host") + " " + msg.get("command") + " " + msg.get("batch_id"));
            //取得返回值的类型
            reMessage = new GenericData.Record(protocol.getType("responseType"));
            reMessage.put("responseName", "The group of Batch job parameters are received!! ");
            oozieclient.submitWorkFlow(msg.get("oozie").toString(), msg.get("path").toString(),
                    msg.get("user").toString(), msg.get("host").toString(),
                    msg.get("command").toString(), Integer.parseInt(msg.get("batch_id").toString()));
        } else if (message.getName().equals("kill")) {
            GenericRecord msg = (GenericRecord) req.get("message");
            System.out.print("接收到数据：");
            System.out.println(msg.get("oozie") + " " + msg.get("path") + " " +
                    msg.get("id") + msg.get("nameNode") + msg.get("jobTracker"));
            //取得返回值的类型
            reMessage = new GenericData.Record(protocol.getType("responseType"));
            reMessage.put("responseName", "The group of yarn killing parameters are received!! ");
            oozieclient.submitWorkFlow4Kill(msg.get("oozie").toString(), msg.get("path").toString(),
                    msg.get("id").toString(), msg.get("nameNode").toString(), msg.get("jobTracker").toString());
        }else if (message.getName().equals("etl")) {
            GenericRecord msg = (GenericRecord) req.get("message");
            System.out.print("接收到数据：");
            System.out.println(msg.get("oozie") + " " + msg.get("path") + " " + msg.get("user")
                    + " " + msg.get("host") + " " + msg.get("command") + " " + msg.get("name"));
            //取得返回值的类型;
            reMessage = new GenericData.Record(protocol.getType("responseType"));
            reMessage.put("responseName", "The group of etlWithKafka job parameters are received!! ");
            oozieclient.submitWorkFlow(msg.get("oozie").toString(), msg.get("path").toString(),
                    msg.get("user").toString(), msg.get("host").toString(),
                    msg.get("command").toString(), msg.get("name").toString());
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
        String protocol = args[0];
        int port = Integer.parseInt(args[1]);
        new Server(Protocol.getProtocol(protocol + ".avpr"), port).run();
        //new Server(Protocol.getProtocol("ml.avpr"), 9090).run();
    }
}
