package com.javarush.test.level39.lesson09.big01;

import java.nio.file.Paths;


public class Solution {

    public static void main(String[] args) {

        LogParser logParser = new LogParser(Paths.get("src/com/javarush/test/level39/lesson09/big01/logs"));
        System.out.println(logParser.execute("get ip for user = \"Eduard Petrovich Morozko\" and date between \"11.12.2013 0:00:00\" and \"03.01.2014 23:59:59\""));

    }
}
