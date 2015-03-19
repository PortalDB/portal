#!/bin/bash
filename="$1"
dos2unix -k -o $1 #not needed if config file in unix format already
exec 6< $1
read line1 <&6
read line2 <&6
read line3 <&6
IFS=' ' read -ra STRIP <<< "$line1"
ITER="${STRIP[1]}"
IFS=' ' read -ra STRIP <<< "$line2"
DATA="${STRIP[1]}"
IFS=' ' read -ra STRIP <<< "$line3"
STRAT="${STRIP[1]}"
COUNTER=0
while read -r line
do
	if [ "$COUNTER" -gt 1 ]; then #make this number numArgs in config - 1
		IFS=' ' read -ra STRIP <<< "$line"
		QUERY="${STRIP[0]}"
		for (( i=1; i <= $ITER; i++ ))
		do
			(
				printf "$QUERY,$i,"
				OUTPUT="$(run $line "--data" $DATA "--strategy" $STRAT)" #where we adjust based on how you run the driver
				sed -n '$p' <<< "$OUTPUT"
			) >> results.csv
		done
	fi
	let COUNTER=1+COUNTER
done < "$filename"
exec 6<&-
