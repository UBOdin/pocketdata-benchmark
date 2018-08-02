package com.example.benchmark_withjson;

import android.app.ActivityManager;
import android.content.Context;
import android.os.Environment;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Utils {

    static JSONObject workloadJsonObject;

    // Additional global config parameters:
    static String database;
    static String workload;
    static String governor;
    static String delay;

    public String jsonToString(Context context, int workload){

        String line;
        String finalString = "";

        try {

            InputStream is = context.getResources().openRawResource(workload);
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            while((line = br.readLine()) != null){
                if(!line.contains("sql")) {
                    line = line.replaceAll("\\s+", "");
                    finalString = finalString + line;
                }
                else {
                    finalString = finalString + line;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return finalString;
    }

    public JSONObject jsonStringToObject(String jsonString){
        JSONObject jsonObject = null;
        try {
            jsonObject = new JSONObject(jsonString);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        workloadJsonObject = jsonObject;
        return jsonObject;
    }

    public int sleepThread(int interval) {

	// Adjust delay time if necessary -- default is lognormal distribution, per parameter:
	if (Utils.delay.equals("0ms")) {
		interval = 0;
	}
	if (Utils.delay.equals("1ms")) {
		interval = 1;
	}

//        putMarker("{\"EVENT\":\"DELAY_start\"}");
        try {
            Thread.sleep(interval);
        } catch (Exception e) {
            e.printStackTrace();
//            putMarker("{\"EVENT\":\"DELAY_error\"}");
            return 1;
        }
//        putMarker("{\"EVENT\":\"DELAY_end\"}");
        return 0;
    }


    public Connection jdbcConnection(String dbName) {
        if(dbName == null){
            return null;
        }
        String url =
                "jdbc:sqlite://data/data/com.example.benchmark_withjson/databases/" + dbName;
        Connection con;
        try {
            Class.forName("SQLite.JDBCDriver");


        } catch (java.lang.ClassNotFoundException e) {
            System.err.print("ClassNotFoundException: ");
            System.err.println(e.getMessage());
            return null;
        }

        try {
            con = DriverManager.getConnection(url);

        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

        return con;
    }

    public int findMissingQueries(Context context){

        // Signal calling script that benchmark run has finished:
        try {
            //String path = this.getFilesDir().getPath().toString();
            //System.out.println("FOOBAR Path:  " + path);
            //String file_name = "/results.txt";
            //FileWriter file_writer = new FileWriter(path + file_name);
            FileWriter file_writer = new FileWriter("/data/results.pipe");
            BufferedWriter buffered_writer = new BufferedWriter(file_writer);
            buffered_writer.write("This is a test.  This is only a test.  Testing 357 testing.\n");
            buffered_writer.close();
        } catch(IOException exception) {
            System.out.println("FOOBAR Error on writing unblock signal");
            exception.printStackTrace();
        }

        try {
            BufferedReader br1;
            BufferedReader br2;
            String sCurrentLine;
            List<String> list1 = new ArrayList<>();
            List<String> list2 = new ArrayList<>();
            br1 = new BufferedReader(new FileReader(context.getFilesDir().getPath() + "/testSQL"));
            br2 = new BufferedReader(new FileReader(context.getFilesDir().getPath() + "/testBDB"));
            while ((sCurrentLine = br1.readLine()) != null) {
                list1.add(sCurrentLine);
            }
            while ((sCurrentLine = br2.readLine()) != null) {
                list2.add(sCurrentLine);
            }
            List<String> tmpList = new ArrayList<>(list1);
            tmpList.removeAll(list2);

            tmpList = list2;
            tmpList.removeAll(list1);

            String dbMissingQueries;
            if(list1.size() < list2.size()){
                dbMissingQueries = "notInBDBQueries";
            }
            else if(list1.size() > list2.size()){
                dbMissingQueries = "notInSQLQueries";
            }
            else {
                dbMissingQueries = "bothRanSameQueries";
            }

            for (int i = 0; i < tmpList.size(); i++) {
                File file = new File(context.getFilesDir().getPath() + "/" + dbMissingQueries);
                FileOutputStream fos = context.openFileOutput(file.getName(), Context.MODE_APPEND);
                String temp = tmpList.get(i) + "\n";
                fos.write(temp.getBytes());
                fos.close();
            }


        } catch (IOException e) {
            e.printStackTrace();
            return 1;
        }

        return 0;
    }

    public int putMarker(String mark) {
        PrintWriter outStream = null;
        try{
            FileOutputStream fos = new FileOutputStream("/sys/kernel/debug/tracing/trace_marker");
            outStream = new PrintWriter(new OutputStreamWriter(fos));
            outStream.println(mark);
            outStream.flush();
        }
        catch(Exception e) {
            Log.d("benchmark_withjson",e.toString());
            return 1;
        }
        finally {
            if (outStream != null) {
                outStream.close();
            }
        }
        return 0;
    }

    public boolean doesDBExist(Context context, String dbPath){
        File dbFile = context.getDatabasePath(dbPath);
        return dbFile.exists();
    }

    public long memoryAvailable(Context context){
        ActivityManager.MemoryInfo mi = new ActivityManager.MemoryInfo();
        ActivityManager activityManager = (ActivityManager) context.getSystemService(Context.ACTIVITY_SERVICE);
        activityManager.getMemoryInfo(mi);
        return mi.availMem;
    }

    public void restrictHeapTo50(){
        //noinspection MismatchedReadAndWriteOfArray
        MainActivity.a = new int[25165824];
        for(int i = 0; i < 25165824; i++){
            MainActivity.a[i] = i;
        }
    }

    public void restrictHeapTo25(){
        int temp = 25165824 + 12582912;
        //noinspection MismatchedReadAndWriteOfArray
        MainActivity.a = new int[temp];
        for(int i = 0; i < temp; i++){
            MainActivity.a[i] = i;
        }
    }

    public void restrictHeapTo12_5(){
        int temp = 25165824 + 12582912 + 6291456;
        //noinspection MismatchedReadAndWriteOfArray
        MainActivity.a = new int[temp];
        for(int i = 0; i < temp; i++){
            MainActivity.a[i] = i;
        }
    }

    public void getMaxHeapAppCanUse(Context context){
        Runtime rt = Runtime.getRuntime();
        long maxMemory = rt.maxMemory();
        File file2 = new File(context.getFilesDir().getPath() + "/max_memory");
        FileOutputStream fos2;
        try {
            fos2 = context.openFileOutput(file2.getName(), Context.MODE_APPEND);
            fos2.write(("Max memomy app can use : " + maxMemory + "\n").getBytes());
            fos2.write(("Max kb : " + maxMemory / 1024L + "\n").getBytes());
            fos2.write(("Max mb : " + maxMemory / 1048576L + "\n").getBytes());
            fos2.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }

}
