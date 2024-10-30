package com.chethan.kafka.consumer;

/**
 * Created by Chethan on Oct 29, 2024.
 */

public class BombBlasting {

    static int    c          = 0;
    static String globalData = "Hello";
    static Thread mainThread = Thread.currentThread();

    private static void throwException() {

        System.out.println("Bomb has been planted. Press the button to explode!!!.");


        //addShutdownHook will terminate the main thread
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                globalData = null;
                mainThread.join();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));
    }


    public static void main(String[] args) throws InterruptedException {
        System.out.println("Main method has started the execution");
        System.out.println("Planting the bomb");
        throwException();


        try {
            while (true) {

                System.out.println(globalData.toString() + c);
                c = c + 1;
                Thread.sleep(500);

            }
        } catch (NullPointerException e) {
            System.out.println("XXX BOOM XXX || Bomb Blasted!!!!");
            Thread.sleep(2000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
