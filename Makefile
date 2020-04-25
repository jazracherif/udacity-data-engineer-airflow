# FERNET_KEY=$(shell cat $(fernet.key))
# 	export AIRFLOW__CORE__FERNET_KEY=$(FERNET_KEY) ; \

clean:
	rm -f venv

venv:
	python3 -m venv venv

install: venv
	export AIRFLOW_HOME=~/airflow;  \
	source venv/bin/activate; \
	pip install -r requirements.txt

# start:
# 	export AIRFLOW_HOME=airflow; \
# 	export AIRFLOW__CORE__FERNET_KEY=$(FERNET_KEY) ; \

# 	(airflow scheduler &) ; \
# 	(airflow webserver -p 8080 & )
# 	@echo "Starting..."

# stop:
# 	$$(sh stop.sh)
# 	@echo "Done"