#!/bin/sh

path=`dirname $0`

cp $path/url_list $path/url_temp
PYTHONPATH=`pwd` $path/SaveHandler.py -t 3 -s $path/url_temp
if [ $? -eq 0 ]
then
    rm $path/url_temp
fi

