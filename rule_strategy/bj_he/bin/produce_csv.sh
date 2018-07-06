hive -hiveconf pt=$1 -f get_all_shot_house.sql|tr '\t' ',' > hive_out.csv
python process.py hive_out.csv
