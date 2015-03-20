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
STRATLIST="${STRIP[1]}"
IFS=',' read -ra STRATS <<< "$STRATLIST"
NUMSTRATS="${#STRATS[@]}"
LINECOUNTER=0
while read -r line
do
	#for each query
	if [ "$LINECOUNTER" -gt 2 ]; then #make this number numArgs in config - 1
		IFS=' ' read -ra STRIP <<< "$line"
		QUERY="${STRIP[0]}"
		#for each strat
		for(( j=0; j<$NUMSTRATS; j++ ))
		do
			STRAT="${STRATS[$j]}"
			echo $QUERY "  " $STRAT 
			#for as many times as iterator signifies
			for (( i=1; i <= $ITER; i++ ))
			do
				(
					printf "$QUERY,$STRAT,$i,"
					OUTPUT="$(sbt run $line "--data" $DATA "--partition" $STRAT)" #where we adjust based on how you run the driver
					sed -n '$p' <<< "$OUTPUT"
				) >> results.csv
			done
		done
	fi
	let LINECOUNTER=1+LINECOUNTER
done < "$filename"
exec 6<&-
