import Helper.Util;

public class Test {
    public static void main(String[] args) {
        for (String arg : args)
            System.out.println("arg = " + arg);
        System.out.println("Hello World");
        System.out.println("User");
        System.out.println("User.dir value: "+System.getProperty("user.dir"));
        System.out.println("User.home value: "+System.getProperty("user.home"));
        System.out.println(Util.getProperty("corebos.test.name"));
    }
}
