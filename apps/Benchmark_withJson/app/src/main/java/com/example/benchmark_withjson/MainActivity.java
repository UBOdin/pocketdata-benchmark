package com.example.benchmark_withjson;

import android.app.ActivityManager;
import android.content.Context;
import android.provider.Settings;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import android.app.ActivityManager.*;

import android.util.Log;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import java.io.BufferedWriter;
import java.io.FileWriter;


public class MainActivity extends AppCompatActivity /*implements Runnable*/ {
    static int a[];
    static String PDE = "PocketData";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
/*
        this.run();
    }

    @Override
    public void run() {
        android.os.Process.setThreadPriority(android.os.Process.THREAD_PRIORITY_BACKGROUND);
*/

	final int[] workload_array = {
		R.raw.workload_a_timing_a, R.raw.workload_b_timing_a,
		R.raw.workload_c_timing_a, R.raw.workload_d_timing_a,
		R.raw.workload_e_timing_a, R.raw.workload_f_timing_a,
		R.raw.workload_ia_timing_a, R.raw.workload_ib_timing_a,
		R.raw.workload_ic_timing_a,
	};

	// Map workload handle to ASCII input:
	String[] workloads = {"A", "B", "C", "D", "E", "F", "IA", "IB", "IC"};
	HashMap<String, Integer> load_map = new HashMap<String, Integer>();
	for (int i = 0; i < workloads.length; i++) {
		load_map.put(workloads[i], workload_array[i]);
	}

	// Pull in setup parameters from on-phone shell script:
	FileReader file_reader;
	BufferedReader buffered_reader;
	String input_line = null;
	try {
		file_reader = new FileReader("/data/config.txt");
		buffered_reader = new BufferedReader(file_reader);
		Utils.database = buffered_reader.readLine().toUpperCase();
		Utils.workload = buffered_reader.readLine().toUpperCase();
		Utils.governor = buffered_reader.readLine();
		Utils.delay = buffered_reader.readLine();
		file_reader.close();
	} catch (IOException exception) {
		exception.printStackTrace();
		return;
	}

	Log.d(PDE, "Parameter Database:  " + Utils.database);
	Log.d(PDE, "Parameter Workload:  " + Utils.workload);
//	Log.d(PDE, "Parameter WL handle:  " + load_map.get(Utils.workload));  // sanity check on hashmap
	Log.d(PDE, "Parameter Governor:  " + Utils.governor);
	Log.d(PDE, "Parameter Delay:  " + Utils.delay);

        Utils utils = new Utils();


        long start = System.currentTimeMillis();
        int tester;

        if(!utils.doesDBExist(this,"BDBBenchmark")){
            Log.d(PDE, "Creating DB");
            //Create the databases from the JSON
            CreateDB createDB = new CreateDB(this);
            tester = createDB.create(load_map.get(Utils.workload));
            if(tester != 0){
                this.finishAffinity();
            }
            this.finishAffinity();
        }
        else {
            Log.d(PDE, "DB exists -- Running DB Benchmark");

            String singleJsonString = utils.jsonToString(this, load_map.get(Utils.workload));
            utils.jsonStringToObject(singleJsonString);


            //Run the queries specified in the JSON on the newly created databases
            Queries queries = new Queries(this);
            tester = queries.startQueries();
            if (tester != 0){
                this.finishAffinity();
            }

            //Find what queries were not executed successfully in the SQL or BDB traces
            utils.findMissingQueries(this);
            //Calculate total time of the traces
            long end = System.currentTimeMillis();

            long delta = end - start;
            double elapsedSeconds = delta / 1000.0;

            File file = new File(this.getFilesDir().getPath() + "/time");
            FileOutputStream fos;
            try {
                fos = this.openFileOutput(file.getName(), Context.MODE_APPEND);
                fos.write((elapsedSeconds + "\n").getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
