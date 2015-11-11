#!/bin/bash -x

mkdir -p UserId

let "day=10#`date +'%d'`-1"

if [ ${#day} -eq 1 ]
then
    day=0$day
fi

#echo $day
dir=`date +%Y-%m-`$day
#echo $dir

cd $dir

cp -f ../AnalysisBidInfoLog .

chmod +x AnalysisBidInfoLog

nohup ./AnalysisBidInfoLog bidinfo.log.$dir ../PkgFile.base &

#AnaFiles=`ls "bidinfo.log."$dir`


#echo ${AnaFiles[@]}
#nohup ./AnalysisBidInfoLog 

#for i in ${AnaFiles[@]}; 
#do 
#    #echo $i
#    nohup ./AnalysisBidInfoLog $i ../PkgFile.base 
#done


