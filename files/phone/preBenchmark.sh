
# Disable SELinux:
setenforce 0

# Set up input parameter file to configure app benchmark:
config_file="/data/config.txt"
rm $config_file
#printf "%s\n%s\n%s\n%s\n" $1 $2 $3 $4 > $config_file # 1 parameter per line
# ^shell printf command MIA.  So:
echo $1 > $config_file
echo $2 >> $config_file
echo $3 >> $config_file
echo $4 >> $config_file
chmod 777 $config_file

am start -n com.example.benchmark_withjson/com.example.benchmark_withjson.MainActivity
sleep 70
