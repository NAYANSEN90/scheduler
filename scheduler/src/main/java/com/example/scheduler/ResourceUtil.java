package com.example.scheduler;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;

public class ResourceUtil {

    private static Logger logger = LoggerFactory.getLogger(ResourceUtil.class);


    public static String fetchLUA(String fileName){
        String resource = null;
        FileInputStream stream = null;

        try {
            stream = new FileInputStream(
                               new File(
                               ClassLoader.getSystemClassLoader().getResource(fileName).getFile()));

            if(stream != null){
                resource = stream.toString();
            }
        }catch (Exception e){
            logger.error(e);

        } finally {
            if(stream != null){
                try {
                    stream.close();
                }catch (Exception e){
                    // ignore
                }
            }
        }

        return resource;
    }
}
