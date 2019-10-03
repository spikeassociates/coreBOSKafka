package model;

import helper.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Modules {
//    String moduleName;
//    ArrayList<String> fieldsConsiderate;
//    ArrayList<String> fieldsDoQuery;

    public Map<String, Map<String, ArrayList<String>>> modules;

    public boolean exist(String param) {
        return modules.containsKey(param);
    }

    public ArrayList<String> getFieldsConsiderate(String param) {
        if (!exist(param)) {
            return null;
        }
        return modules.get(param).get("fieldsConsiderate");
    }

    public ArrayList<String> getFieldsDoQuery(String param) {
        if (!exist(param)) {
            return null;
        }
        return modules.get(param).get("fieldsDoQuery");
    }


    public void createDemo() {
        modules = new HashMap<>();

        //contacts example
        String moduleName = Util.elementTypeCONTACTS;
        ArrayList<String> fieldsConsiderate = new ArrayList<>();
        fieldsConsiderate.add("firstname");
        fieldsConsiderate.add("lastname");
        fieldsConsiderate.add("phone");
        fieldsConsiderate.add("email");
        fieldsConsiderate.add("birthday");
        ArrayList<String> fieldsDoQuery = new ArrayList<>();
        fieldsDoQuery.add("firstname");
        fieldsDoQuery.add("lastname");
        fieldsDoQuery.add("phone");
        fieldsDoQuery.add("email");
        fieldsDoQuery.add("birthday");
        Map map = new HashMap();
        map.put("fieldsConsiderate", fieldsConsiderate);
        map.put("fieldsDoQuery", fieldsDoQuery);
        modules.put(moduleName, map);

        //accounts example
        moduleName = Util.elementTypeACCOUNTS;
        fieldsConsiderate = new ArrayList<>();
        fieldsConsiderate.add("accountname");
        fieldsConsiderate.add("website");
        fieldsConsiderate.add("phone");
        fieldsConsiderate.add("email1");
        fieldsConsiderate.add("email2");
        fieldsConsiderate.add("fax");
        fieldsDoQuery = new ArrayList<>();
        fieldsDoQuery.add("website");
        fieldsDoQuery.add("lastname");
        fieldsDoQuery.add("phone");
        fieldsDoQuery.add("email1");
        fieldsDoQuery.add("email2");
        fieldsDoQuery.add("fax");
        map = new HashMap();
        map.put("fieldsConsiderate", fieldsConsiderate);
        map.put("fieldsDoQuery", fieldsDoQuery);


        modules.put(moduleName, map);


//        resulit is
//         {
//  "modules": {
//    "Contacts": {
//      "fieldsConsiderate": [
//        "firstname",
//        "lastname",
//        "phone",
//        "email",
//        "birthday"
//      ],
//      "fieldsDoQuery": [
//        "firstname",
//        "lastname",
//        "phone",
//        "email",
//        "birthday"
//      ]
//    },
//    "Accounts": {
//      "fieldsConsiderate": [
//        "accountname",
//        "website",
//        "phone",
//        "email1",
//        "email2",
//        "fax"
//      ],
//      "fieldsDoQuery": [
//        "website",
//        "lastname",
//        "phone",
//        "email1",
//        "email2",
//        "fax"
//      ]
//    }
//  }
//}

//            write in application.prorerties like this
//        corebos.test.modules={"modules":{"Contacts":{"fieldsConsiderate":["firstname","lastname","phone","email","birthday"],"fieldsDoQuery":["firstname","lastname","phone","email","birthday"]},"Accounts":{"fieldsConsiderate":["accountname","website","phone","email1","email2","fax"],"fieldsDoQuery":["website","lastname","phone","email1","email2","fax"]}}}

    }

}


