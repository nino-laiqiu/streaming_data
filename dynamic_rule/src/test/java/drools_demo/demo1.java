package drools_demo;


import org.apache.commons.io.FileUtils;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.utils.KieHelper;

import java.io.File;
import java.io.IOException;

public class demo1 {
    public static void main(String[] args) throws IOException {


        String drlString = FileUtils.readFileToString(new File("F:\\project\\java project\\streaming_data\\dynamic_rule\\src\\main\\resources\\rule1\\demo1.drl"), "utf-8");
        KieHelper kieHelper = new KieHelper();
        KieHelper content = kieHelper.addContent(drlString, ResourceType.DRL);
        KieSession session = kieHelper.build().newKieSession();
        Student s = new Student(2);
        session.insert(s);
        session.insert(new Teacher(100));
        session.fireAllRules();
        session.dispose();
        System.out.println(s.getAge());

    }

}
