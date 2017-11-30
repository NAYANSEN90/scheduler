package com.example.scheduler;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;

public class ResourceUtil {

    private static Logger logger = LoggerFactory.getLogger(ResourceUtil.class);


    public static String fetchLUA(String fileName){
        String resource = null;

        try {
            resource = new FileInputStream(
                               new File(
                               ClassLoader.getSystemClassLoader().getResource(fileName).getFile())).toString();

        }catch (Exception e){
            logger.error(e);
        }

        return resource;
    }
}
