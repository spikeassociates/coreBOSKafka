import consumer.SiaeConsumer;

public class SiaeConsumerExe {
    public static void main(String[] args) {
        try {
            new SiaeConsumer().init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
