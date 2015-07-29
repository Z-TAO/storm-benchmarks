package com.utils;

import java.io.*;
import java.util.Scanner;

/**
 * Created by Z TAO on 7/29/2015.
 */
public class LineReader {
    int currentsize = 0;
    String [] data;
    FileReader f;
    public LineReader(String name) throws IOException{
        File ff = new File(name);
        f = new FileReader(ff);
        char [] temp = new char[(int)ff.length()];
        f.read(temp);
        String x = new String(temp);
        data = x.split("\\n");

    }

    public String getNextLine() throws IOException{
        /*String line = "";
        byte ch;
        while (f.hasNextByte()){
            ch = f.nextByte();
            if (ch == '\n'){
                break;
            }
            line += ch;
        }

        if (line.isEmpty()){
            return null;
        }

        return line;
*/
        if (currentsize!=data.length){
            return data[currentsize++];
        }else{
            return null;
        }
    }
}
