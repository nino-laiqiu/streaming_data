package jdbc_demo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Iterable_demo1 {


    public static void main(String[] args) {
        List<String> l = new ArrayList<>();
        for (int i=0;i<10;i++){
            l.add(Integer.toString(i));
        }

        Iterator<String> it = l.iterator();
        while (it.hasNext()) {
            System.out.print(it.next() + ",");
        }

        while (it.hasNext()) {
            System.out.print(it.next() + ",");
        }

    }
}
