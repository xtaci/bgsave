#!/bin/bash
set -e
NSQD_HOST="http://172.17.42.1:4151"
REDIS_HOST="172.17.42.1:6379"
MONGODB_URL="mongodb://172.17.42.1/mydb"
SAVE_DELAY=15
case $1 in 
	production)
		NSQD_HOST="http://172.17.42.1:4151"
		REDIS_HOST="172.17.42.1:6379"
		MONGODB_URL="mongodb://172.17.42.1/mydb"
		SAVE_DELAY=600
		;;
	testing)
		NSQD_HOST="http://172.17.42.1:4151"
		REDIS_HOST="172.17.42.1:6379"
		MONGODB_URL="mongodb://172.17.42.1/mydb"
		SAVE_DELAY=60
		;;
esac
export NSQD_HOST=$NSQD_HOST
export REDIS_HOST=$REDIS_HOST
export MONGODB_URL=$MONGODB_URL
export SAVE_DELAY=$SAVE_DELAY
echo "NSQD_HOST set to:" $NSQD_HOST
echo "REDIS_HOST set to:" $REDIS_HOST
echo "MONGODB_URL set to:" $MONGODB_URL
echo "SAVE_DELAY set to:" $SAVE_DELAY
$GOBIN/bgsave
