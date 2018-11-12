package com.datageek.oozie;

import com.datageek.oozie.utils.PropertiesReader;
import com.datageek.oozie.utils.Protocol;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.generic.GenericRequestor;

import java.io.IOException;
import java.net.URL;

public class Client2 {
    private org.apache.avro.Protocol protocol = null;
    private String host = null;
    private int port = 0;
    private int count = 1;

    public Client2(org.apache.avro.Protocol protocol, String host, int port, int count) {
        this.protocol = protocol;
        this.host = host;
        this.port = port;
        this.count = count;
    }

    public Client2(org.apache.avro.Protocol protocol, String host, int port) {
        this.protocol = protocol;
        this.host = host;
        this.port = port;
    }

    public long sendMLParameters(String type, String name, String id) throws Exception {
        GenericRecord requestData = new GenericData.Record(protocol.getType("datageek"));
        requestData.put("type", type);
        requestData.put("start", "");
        requestData.put("end", "");
        requestData.put("frequency", "");
        requestData.put("subtype", "");
        requestData.put("arg1", name);
        requestData.put("arg2", id);

        GenericRecord request = new GenericData.Record(protocol.getMessages().get("datageek").getRequest());
        request.put("message", requestData);

        Transceiver t = new HttpTransceiver(new URL("http://" + host + ":" + port));
        GenericRequestor requestor = new GenericRequestor(protocol, t);

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Object result = requestor.request("datageek", request);
            if (result instanceof GenericData.Record) {
                GenericData.Record record = (GenericData.Record) result;
                System.out.println(record);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println((end - start)+"ms");
        return end - start;
    }

    public long sendBatchParameters(String type, String id) throws Exception {
        GenericRecord requestData = new GenericData.Record(protocol.getType("datageek"));
        requestData.put("type", type);
        requestData.put("start", "");
        requestData.put("end", "");
        requestData.put("frequency", "");
        requestData.put("subtype", "");
        requestData.put("arg1", id);
        requestData.put("arg2", "");

        GenericRecord request = new GenericData.Record(protocol.getMessages().get("datageek").getRequest());
        request.put("message", requestData);

        Transceiver t = new HttpTransceiver(new URL("http://" + host + ":" + port));
        GenericRequestor requestor = new GenericRequestor(protocol, t);

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Object result = requestor.request("datageek", request);
            if (result instanceof GenericData.Record) {
                GenericData.Record record = (GenericData.Record) result;
                System.out.println(record);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println((end - start)+"ms");
        return end - start;
    }

    public long sendETLParameters(String type, String option) throws Exception {
        GenericRecord requestData = new GenericData.Record(protocol.getType("datageek"));
        requestData.put("type", type);
        requestData.put("start", "");
        requestData.put("end", "");
        requestData.put("frequency", "");
        requestData.put("subtype", "");
        requestData.put("arg1", option);
        requestData.put("arg2", "");

        GenericRecord request = new GenericData.Record(protocol.getMessages().get("datageek").getRequest());
        request.put("message", requestData);

        Transceiver t = new HttpTransceiver(new URL("http://" + host + ":" + port));
        GenericRequestor requestor = new GenericRequestor(protocol, t);

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Object result = requestor.request("datageek", request);
            if (result instanceof GenericData.Record) {
                GenericData.Record record = (GenericData.Record) result;
                System.out.println(record);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println((end - start)+"ms");
        return end - start;
    }

    public long sendKillParameters(String type, String id) throws Exception {
        GenericRecord requestData = new GenericData.Record(protocol.getType("datageek"));
        requestData.put("type", type);
        requestData.put("start", "");
        requestData.put("end", "");
        requestData.put("frequency", "");
        requestData.put("subtype", "");
        requestData.put("arg1", id);
        requestData.put("arg2", "");

        GenericRecord request = new GenericData.Record(protocol.getMessages().get("datageek").getRequest());
        request.put("message", requestData);

        Transceiver t = new HttpTransceiver(new URL("http://" + host + ":" + port));
        GenericRequestor requestor = new GenericRequestor(protocol, t);

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Object result = requestor.request("datageek", request);
            if (result instanceof GenericData.Record) {
                GenericData.Record record = (GenericData.Record) result;
                System.out.println(record);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println((end - start)+"ms");
        return end - start;
    }

    public long sendCoordinatorKill(String type, String id) throws Exception {
        GenericRecord requestData = new GenericData.Record(protocol.getType("datageek"));
        requestData.put("type", type);
        requestData.put("start", "");
        requestData.put("end", "");
        requestData.put("frequency", "");
        requestData.put("subtype", "");
        requestData.put("arg1", id);
        requestData.put("arg2", "");

        GenericRecord request = new GenericData.Record(protocol.getMessages().get("datageek").getRequest());
        request.put("message", requestData);

        Transceiver t = new HttpTransceiver(new URL("http://" + host + ":" + port));
        GenericRequestor requestor = new GenericRequestor(protocol, t);

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Object result = requestor.request("datageek", request);
            if (result instanceof GenericData.Record) {
                GenericData.Record record = (GenericData.Record) result;
                System.out.println(record);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println((end - start)+"ms");
        return end - start;
    }

    public long sendParameters(String type, String name, String id) throws Exception {
        GenericRecord requestData = new GenericData.Record(protocol.getType("datageek"));
        requestData.put("type", type);
        requestData.put("start", "");
        requestData.put("end", "");
        requestData.put("frequency", "");
        requestData.put("subtype", "");
        requestData.put("arg1", name);
        requestData.put("arg2", id);

        GenericRecord request = new GenericData.Record(protocol.getMessages().get("datageek").getRequest());
        request.put("message", requestData);

        Transceiver t = new HttpTransceiver(new URL("http://" + host + ":" + port));
        GenericRequestor requestor = new GenericRequestor(protocol, t);

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Object result = requestor.request("datageek", request);
            if (result instanceof GenericData.Record) {
                GenericData.Record record = (GenericData.Record) result;
                System.out.println(record);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println((end - start)+"ms");
        return end - start;
    }

    public long sendParameters(String type, String id) throws Exception {
        return sendParameters(type, id, "");
    }

    public long sendCoordinatorParameters(String type, String start, String end, String frequency,
                                          String subtype, String arg1, String arg2) throws Exception {
        GenericRecord requestData = new GenericData.Record(protocol.getType("datageek"));
        requestData.put("type", type);
        requestData.put("start", start);
        requestData.put("end", end);
        requestData.put("frequency", frequency);
        requestData.put("subtype", subtype);
        requestData.put("arg1", arg1);
        requestData.put("arg2", arg2);

        GenericRecord request = new GenericData.Record(protocol.getMessages().get("datageek").getRequest());
        request.put("message", requestData);

        Transceiver t = new HttpTransceiver(new URL("http://" + host + ":" + port));
        GenericRequestor requestor = new GenericRequestor(protocol, t);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Object result = requestor.request("datageek", request);
            if (result instanceof GenericData.Record) {
                GenericData.Record record = (GenericData.Record) result;
                System.out.println(record);
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime)+"ms");
        return endTime - startTime;
    }

    public long sendCoordinatorParameters(String type, String start, String end, String frequency,
                                          String subtype, String arg1) throws Exception{
        return sendCoordinatorParameters(type, start, end, frequency, subtype, arg1, "");
    }

    public static void main(String[] args) throws Exception {
        //batch
        /*try {
            PropertiesReader.loadPropertiesByClassPath("conn.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String type = "batch";
        String ip = PropertiesReader.getProperty("ip");
        int port = Integer.parseInt(PropertiesReader.getProperty("port"));
        //String name = "Test";//args[0]
        String batch_id = "1";//Integer.parseInt(args[0]);
        new Client2(Protocol.getProtocol("datageek.avpr"), ip, port)
        .sendParameters(type, batch_id);*/

        //machine learning
        /*try {
            PropertiesReader.loadPropertiesByClassPath("conn.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String ip = PropertiesReader.getProperty("ip");
        int port = Integer.parseInt(PropertiesReader.getProperty("port"));
        String type = "ml";
        String name = "GM";//args[0]
        String id = "14";//args[1]

        new Client2(Protocol.getProtocol("datageek.avpr"), ip, port)
        .sendParameters(type, name, id);*/

        //mlPredict
        try {
            PropertiesReader.loadPropertiesByClassPath("conn.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String type = "mlPredict";
        String ip = PropertiesReader.getProperty("ip");
        int port = Integer.parseInt(PropertiesReader.getProperty("port"));
        //String name = "Test";//args[0]
        String predictId = "uuid";//Integer.parseInt(args[0]);
        new Client2(Protocol.getProtocol("datageek.avpr"), ip, port)
        .sendParameters(type, predictId);

        //yarn kill
        /*try {
            PropertiesReader.loadPropertiesByClassPath("conn.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String ip = PropertiesReader.getProperty("ip");
        int port = Integer.parseInt(PropertiesReader.getProperty("port"));
        String type = "kill";

        String id = "application_1488782145912_0023";//Integer.parseInt(args[0]);
        new Client2(Protocol.getProtocol("datageek.avpr"), ip, port)
                .sendParameters(type, id);*/

        //etl
        /*try {
            PropertiesReader.loadPropertiesByClassPath("conn.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String ip = PropertiesReader.getProperty("ip");
        int port = Integer.parseInt(PropertiesReader.getProperty("port"));

        String type = "etl";
        String option = "-k@2@engine@2@-b@2@172.31.18.14:9092,172.31.18.13:9092,172.31.18.12:9092@2@-t@2@bcm_engine@2@-f@2@bcm_engine@2@-i@2@10@2@-r";
        new Client2(Protocol.getProtocol("datageek.avpr"), ip, port)
        .sendParameters(type, option);*/

        //coordinator
        /*try {
            PropertiesReader.loadPropertiesByClassPath("conn.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String ip = PropertiesReader.getProperty("ip");
        int port = Integer.parseInt(PropertiesReader.getProperty("port"));

        String type = "coordinator";
        String start = "2017-02-22T9:30+0800";
        String end = "2017-02-22T11:00+0800";
        String frequency = "7/30 * * * *";
        String subtype = "batch";
        String arg1 = "1";
        new Client2(Protocol.getProtocol("datageek.avpr"), ip, port)
        .sendCoordinatorParameters(type, start, end, frequency, subtype, arg1);*/

        //coordinatorkill
        /*try {
            PropertiesReader.loadPropertiesByClassPath("conn.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String ip = PropertiesReader.getProperty("ip");
        int port = Integer.parseInt(PropertiesReader.getProperty("port"));

        String type = "coordinatorkill";
        String option = "0000161-170214123146983-oozie-oozi-C";
        new Client2(Protocol.getProtocol("datageek.avpr"), ip, port)
        .sendParameters(type, option);*/
    }
}
