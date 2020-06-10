# How to deploy BASiC(Beam Pool Payment Processor)

## Install Python
Please, install Python using the link below

**Ubuntu:** https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-programming-environment-on-ubuntu-18-04-quickstart

`sudo apt update`

`sudo apt -y upgrade`

`sudo apt install software-properties-common build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev wget python3-dev python3-setuptools`

`sudo apt install -y python3-pip`

`pip3 install -r requirements.txt`

## Do auto-updates
*Open Crontab* `crontab -e`

Write down next cmd to launch payment processor every 3 minutes:

`*/3 * * * * cd /root/basic;python3 auto_payment.py`


All code released with no warranty and no support assistance.
