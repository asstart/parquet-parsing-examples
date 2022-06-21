package dev.asstart;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        var file = "";
        printRowGroupRowsCount(file);
        printFooter(file);
        printParquetData(file);
        printParquetDicts(file);
    }

    private static void printParquetDicts(String path) {
        var pp = new ParquetParser(path);
        try {
            pp.printParquetDicts();
        } catch (IOException ex) {
            System.out.println("Can't print dictionaries, " + ex.getMessage());
            ex.printStackTrace(System.err);
        }
    }

    private static void printParquetData(String path) {
        var pp = new ParquetParser(path);
        try {
            pp.printParquetData();
        } catch (IOException ex) {
            System.out.println("Can't print data, " + ex.getMessage());
            ex.printStackTrace(System.err);
        }
    }

    private static void printRowGroupRowsCount(String path) {
        var pp = new ParquetParser(path);
        try {
            pp.printRowGroupsRowsCount();
        } catch (IOException ex) {
            System.out.println("Can't print row group rows count, " + ex.getMessage());
            ex.printStackTrace(System.err);
        }
    }

    private static void printFooter(String path) {
        var pp = new ParquetParser(path);
        try {
            pp.printParquetFooter();
        } catch (IOException ex) {
            System.out.println("Can't print footer, " + ex.getMessage());
            ex.printStackTrace(System.err);
        }
    }

}
