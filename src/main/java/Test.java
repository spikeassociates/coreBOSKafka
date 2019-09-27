import Helper.Util;

public class Test {

    public static void main(String[] args) {

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
