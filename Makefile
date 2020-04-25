
clean:
	rm -f venv

venv:
	python3 -m venv venv

install: venv
	export AIRFLOW_HOME=./airflow;  \
	export AIRFLOW__CORE__FERNET_KEY=$(cat fernet.key) \
	source venv/bin/activate; \
	pip install -r requirements.txt;
	airflow initdb
