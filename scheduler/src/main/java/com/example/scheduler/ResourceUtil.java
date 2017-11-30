package com.example.scheduler;

import com.google.common.io.Resources;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.IOUtils;

import java.net.URL;
import java.nio.charset.Charset;


public class ResourceUtil {

    private static Logger logger = LoggerFactory.getLogger(ResourceUtil.class);


    public static String fetchLUA(String fileName){
        String resource = null;

        try{
            URL url = Resources.getResource(fileName);
            resource = Resources.toString(url, Charset.defaultCharset());

        }catch (Exception e){
            logger.error(e);
        }

        return resource;
    }
}
