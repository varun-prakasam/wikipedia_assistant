# Bash script to execute on server after ssh'ing in:
#
# 1. Set up workspace directory with read-write permissions
#
# 2. Install git and clone code repository
#
# 3. Install all other RHEL and Python packages required
#
# 4. Export PYTHONPATH and run app.py

cd /opt/

sudo mkdir workspaces

sudo chmod ugo+rw workspaces/

cd workspaces/

sudo yum -y install git

git clone https://github.com/varun-prakasam/wikipedia_assistant.git

sudo yum -y install python3

sudo yum -y install mysql

sudo pip3 install pymysql

sudo pip3 install pandas

sudo pip3 install flask

export PYTHONPATH=/opt/workspaces/wikipedia_assistant/

# Optional step to initialize database - If database is already initialized with all meta tables and functional, then skip this step.
# python3 wikipedia_assistant/db/init.py

nohup python3 wikipedia_assistant/web_service/app.py > log.txt &

# To view the app's log
tail -f log.txt


# To kill background process running web service:
#
# ps -auwx | grep app.py | grep -v grep
# kill -9 {PID}


# To rerun app.py:
#
# export PYTHONPATH=/opt/workspaces/wikipedia_assistant/
# nohup python3 wikipedia_assistant/web_service/app.py > log.txt &


# To pull latest code and rerun app.py:
#
# cd wikipedia_assistant
# git pull
# cd ..
# export PYTHONPATH=/opt/workspaces/wikipedia_assistant/
# nohup python3 wikipedia_assistant/web_service/app.py > log.txt &

