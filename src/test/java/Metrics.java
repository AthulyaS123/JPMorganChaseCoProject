import java.util.Scanner;

public class Metrics {
    public static void main(String[] args) {

        States state = States.START;
        Scanner sc = new Scanner(System.in);
        while (true) {
            int i = sc.nextInt();
            if (i == 0) {
                state = States.END;
            }
            if (i == 1) {
                state = States.RUNNING;
            }

            switch (state) {
                case START:
                    break;
                case RUNNING:
                    System.out.println("running");
                    break;
                case END:
                    System.out.println("end");
                    break;
                default:
                    System.out.println("ur MOM");
                    break;
            }
        }
    }
}









