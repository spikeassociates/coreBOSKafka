import Helper.HelperModelData;
import model.KeyData;
import model.ValueData;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestKEyValueJson {
    @Test
    public void testKeyJson() {
        KeyData keyData = new KeyData();
        keyData.SquareId = "577";
        assertEquals("{\"SquareId\":\"577\"}", HelperModelData.getJson(keyData));
    }

    @Test
    public void testJsonKey() {

        KeyData keyData = KeyData.getObjectFromJson("{\"SquareId\":\"577\"}");
        assertEquals(keyData.SquareId, "577");
    }

    @Test
    public void testJsonValue() {
        ValueData valueData = ValueData.getObjectFromJson(new File("C:\\Users\\User\\Desktop\\corebos\\src\\test\\file_test\\jsonFile.json"));
        assertEquals(valueData.user.firstName.equals("Ardit"), true);
    }

}
