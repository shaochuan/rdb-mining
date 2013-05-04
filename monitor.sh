#!/bin/bash
while true
do
	count=`ps aux | grep rethinkdb | wc -l`
	if [ $count -lt 7 ]
	then
		/etc/init.d/rethinkdb start
	fi
	sleep 5
done
