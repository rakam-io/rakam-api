package org.rakam;

import com.google.inject.Module;
import org.rakam.bootstrap.SystemRegistry;
import org.rakam.util.JsonHelper;

import java.io.IOException;
import java.util.Set;

public class SystemRegistryGenerator {

    public static void main(String[] args) throws IOException {
        Set<Module> allModules = ServiceStarter.getModules();
        SystemRegistry systemRegistry = new SystemRegistry(allModules, allModules);
        System.out.println(JsonHelper.encode(systemRegistry));
//        File file = new File("registry.json");
//        if (!file.exists()) {
//            file.createNewFile();
//        }
//        FileOutputStream fileOutputStream = new FileOutputStream(file);
//        fileOutputStream.write(JsonHelper.encode(systemRegistry).getBytes(Charset.forName("UTF-8")));
//        fileOutputStream.close();
    }
}
