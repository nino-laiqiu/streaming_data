package List_demo;

import java.util.ArrayList;
import java.util.List;

public class List_demo {
    public static void main(String[] args) {
        List<Name> list = new ArrayList<Name>();
        list.add(new Name("1","99999"));
        list.add(new Name("2","99991"));
        list.add(new Name("小米","99991"));

        int of = list.indexOf("1");
        System.out.println(of);
    }
}
