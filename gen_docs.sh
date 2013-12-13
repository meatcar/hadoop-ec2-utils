#!/bin/bash

sudo rm -rf html*
sudo epydoc ec2_allspark.py
tar -zcvf html.tar.gz html

