if [ "$1" = "-h" ];then
sh run.sh local 25 -h
exit 0
fi
cd `dirname $0`
#sh run.sh yarn-cluster 10 -k engine -b 172.31.18.14:9092,172.31.18.13:9092,172.31.18.12:9092 -t bcm_engine  -f bcm_engine  -i 10 -r
option=$1
echo "option"=$option >> batch.log
sh run.sh yarn-cluster 25 "$option"
