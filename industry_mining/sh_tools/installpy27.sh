#!/bin/bash
sudo yum --enablerepo=ius install python27 python27-pip python27-psutil python27-virtualenv python27-backports-ssl_match_hostname python27-devel
sudo cp  /usr/bin/python2.7 /usr/bin/python
pip2.7 install scipy numpy sklearn
