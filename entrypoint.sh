#!/usr/bin/env bash
nohup /usr/bin/bolt-monitor & >> /var/log/bolt-monitor.log
/usr/bin/heketi $1