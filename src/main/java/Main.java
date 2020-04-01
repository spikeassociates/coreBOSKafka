import helper.Util;

import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) {

        Map map = new HashMap();
        map.put("Ss", "sdf");

        if (map.get("S") != null) {
            String s = map.get("S").toString().replace("s", "a");
            System.out.println("s = " + s);
        }
        for (String arg : args)
            System.out.println("arg = " + arg);
        System.out.println("Hello World");
        System.out.println("User");
        System.out.println("User.dir value: " + Util.coreBossDir);
        System.out.println("User.home value: " + System.getProperty("user.home"));
        System.out.println(Util.getProperty("corebos.json"));
        System.out.println(Util.getProperty("corebos.test.name"));

    }
}
