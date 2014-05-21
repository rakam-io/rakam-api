package org.rakam.cache;

import org.rakam.constant.AggregationType;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by buremba on 18/01/14.
 */
public class CacheQueryBuilder {
    public static String createHash(String projectId, AggregationType aggType, String selectField, String groupBy, Map<String, String> filters) {
        /*ByteArrayOutputStream baos = new ByteArrayOutputStream();

        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(projectId);
            oos.writeObject(selectField);
            oos.writeObject(groupBy);
            //oos.writeObject(filters);
            oos.close();
        } catch (IOException e) {
            return null;
        }


        return new String(baos.toByteArray());*/
        //String jso = JSONObject.toJSONString(filters);
         //new String(Base64.encodeBase64(jso.getBytes()));

        return projectId+aggType.id+selectField+groupBy;
    }

    public static void main(String [] args) {
        HashMap<String, String> n = new HashMap<>();
        n.put("test","test");

        long startTime = System.currentTimeMillis();
        for(int i=0; i<100000000; i++)
            createHash("nsdandna", AggregationType.AVERAGE_X, "sfsdf", "dgdfg", n);
        long finishTime = System.currentTimeMillis();
        System.out.println(finishTime-startTime);

    }
}
