#!/bin/bash

echoerr() { cat <<< "$@" 1>&2; }

filename="$1"
#dos2unix -k -o $1 #not needed if config file in unix format already
exec 6< $1
read line1 <&6
read line2 <&6
read line3 <&6
read line4 <&6
read line5 <&6
IFS=' ' read -ra STRIP <<< "$line1"
ITER="${STRIP[1]}"
IFS=' ' read -ra STRIP <<< "$line2"
GTYPE="${STRIP[1]}"
IFS=' ' read -ra STRIP <<< "$line3"
DATA="${STRIP[1]}"
IFS=' ' read -ra STRIP <<< "$line4"
STRATLIST="${STRIP[1]}"
IFS=',' read -ra STRATS <<< "$STRATLIST"
NUMSTRATS="${#STRATS[@]}"
IFS=' ' read -ra STRIP <<< "$line5"
PARTSLIST="${STRIP[1]}"
IFS=',' read -ra PARTS <<< "$PARTSLIST"
NUMPARTS="${#PARTS[@]}"
LINECOUNTER=-1
while read -r line
do
	#for each query
	if [ "$LINECOUNTER" -gt 3 ]; then #make this number numArgs in config - 1
		IFS=' ' read -ra STRIP <<< "$line"
		QUERY="${STRIP[0]}"

		#for each partition num passed
        for(( k=0; k<$NUMPARTS; k++ ))
        do
            PARTITION="${PARTS[$k]}"

            #for each strat
		    for(( j=0; j<$NUMSTRATS; j++ ))
		    do
                STRAT="${STRATS[$j]}"
                echo $QUERY " " $STRAT " " $PARTITION "partitions"
                #for as many times as iterator signifies

                typeParam="--type"
                dataParam="--data"
                partitionParam="--partition"
                runCommand="sbt \"run $line $dataParam $DATA $typeParam $GTYPE $partitionParam $STRAT $PARTITION\"" #where we adjust based on how you run the driver
			    for (( i=1; i <= $ITER; i++ ))
                do
				    (
                        printf "$QUERY,$STRAT,$PARTITION,$i,"
                        OUTPUT="$(eval $runCommand)"
		        echoerr $OUTPUT
                        grep -E 'Final Runtime' <<< "$OUTPUT"
				    ) >> results.csv
			    done
		    done
        done
	fi
	let LINECOUNTER=1+LINECOUNTER
done < "$filename"
exec 6<&-
