import helper.Util;
import model.KeyData;
import model.ValueData;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestKEyValueJson {
    @Test
    public void testKeyJson() {
        KeyData keyData = new KeyData();
        keyData.operation = "577";
        keyData.module = "577";
        assertEquals("{\"operation\":\"577\",\"module\":\"577\"}", Util.getJson(keyData));
    }

    @Test
    public void testJsonKey() {

        KeyData keyData = Util.getObjectFromJson("{\"operation\":\"577\",\"module\":\"577\"}", KeyData.class);
        assertEquals(keyData.operation, "577");
    }

    @Test
    public void testJsonValue() {
        ValueData valueData = Util.getObjectFromJson(new File("C:/Users/User/Desktop/corebos/src/test/file_test/jsonFile.json"), ValueData.class);
        assertEquals(valueData.user.firstName.equals("Ardit"), true);
    }

}
