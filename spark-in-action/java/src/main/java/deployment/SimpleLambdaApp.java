package deployment;

import java.util.Arrays;
import java.util.List;

public class SimpleLambdaApp {

    public static void main(String[] args) {
        List<String> frenchNameList = Arrays.asList("Georges", "Claude",
                "Philippe", "Pierre", "François", "Michel", "Bernard", "Guillaume",
                "André", "Christophe", "Luc", "Louis");

        frenchNameList.forEach(
                name -> System.out.println(name + " and Jean-" + name + " are different")
        );

        System.out.println("-----");

        frenchNameList.forEach(
                name -> {
                    String message = name + " and Jean-";
                    message += name;
                    message += " are diffrent";
                    System.out.println(message);
                }
        );
    }
}
