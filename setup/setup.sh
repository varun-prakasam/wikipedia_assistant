# Bash script to execute on new server after ssh'ing in:
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

mkdir archive

sudo yum -y install git

git clone https://github.com/varun-prakasam/wikipedia_assistant.git

sudo yum -y install python3

sudo yum -y install mysql

sudo pip3 install pymysql

sudo pip3 install pandas

sudo pip3 install flask

export PYTHONPATH=/opt/workspaces/wikipedia_assistant/

# Optional step to initialize empty/new database. If database is already initialized with all meta tables, then skip this step.
# python3 wikipedia_assistant/db/init.py

nohup python3 wikipedia_assistant/web_service/app.py > log_wiki_assistant.txt &

# To view the log
tail -f log_wiki_assistant.txt


# To kill background process running web service:
#
# ps -auwx | grep app.py | grep -v grep
# kill -9 {PID}


# To rerun:
#
# mv log_wiki_assistant.txt archive/log_wiki_assistant_prev.txt
# export PYTHONPATH=/opt/workspaces/wikipedia_assistant/ && nohup /bin/python3 /opt/workspaces/wikipedia_assistant/web_service/app.py > /opt/workspaces/log_wiki_assistant.txt &


# To pull latest code and rerun:
#
# cd wikipedia_assistant
# git pull
# cd ..
# mv log_wiki_assistant.txt archive/log_wiki_assistant_prev.txt
# export PYTHONPATH=/opt/workspaces/wikipedia_assistant/ && nohup /bin/python3 /opt/workspaces/wikipedia_assistant/web_service/app.py > /opt/workspaces/log_wiki_assistant.txt &


# To run the ETL pipeline job manually
#
# export PYTHONPATH=/opt/workspaces/wikipedia_assistant/ && nohup /bin/python3 /opt/workspaces/wikipedia_assistant/etl/run_etl_pipeline.py > /opt/workspaces/log_etl.txt &


# To schedule ETL job to run daily at 2 AM - Job checks if there is a new wiki dump and starts ETL if there is
#
# Insert into crontab -e following
# 0 2 * * * export PYTHONPATH=/opt/workspaces/wikipedia_assistant/ && nohup /bin/python3 /opt/workspaces/wikipedia_assistant/etl/run_etl_pipeline.py > /opt/workspaces/log_etl.txt