#!/bin/sh

cp url_list url_temp
./SaveHandler.py 1 url_temp
if [ $? -eq 0 ]
then
    rm url_temp
fi

