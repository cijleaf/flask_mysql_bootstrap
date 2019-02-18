#!/bin/bash
git pull
sudo find /var/cache/nginx -type f -delete
sudo service nginx stop
sudo service nginx start
sudo service uaereal stop
sudo service uaereal start
echo " -- UPDATED"