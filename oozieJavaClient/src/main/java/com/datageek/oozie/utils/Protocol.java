package com.datageek.oozie.utils;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class Protocol {

    public static org.apache.avro.Protocol getProtocol(String path) {
        org.apache.avro.Protocol protocol = null;
        try {
            URL url = Protocol.class.getClassLoader().getResource(path); //"message.avpr"
            protocol = org.apache.avro.Protocol.parse(new File(url.getPath()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return protocol;
    }
}
