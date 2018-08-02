rm /data/data/com.example.benchmark_withjson/databases/SQLBenchmark
rm /data/data/com.example.benchmark_withjson/databases/SQLBenchmark-journal
rm /data/data/com.example.benchmark_withjson/databases/BDBBenchmark
rm /data/data/com.example.benchmark_withjson/databases/BDBBenchmark-journal/*

if [ -e /data/DB_CONFIG ]; then
	mv /data/DB_CONFIG /data/data/com.example.benchmark_withjson/databases/BDBBenchmark-journal/
fi

# Paranoia...
rm /data/DB_CONFIG

