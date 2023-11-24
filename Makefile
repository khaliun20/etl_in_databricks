install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

lint:
	pylint --disable=R,C --ignore-patterns=test/test_.*?py mylib/*.py
	# disables refactor and conection messages
	# --ignore-patterns - tells it to ignore these when completing linting 
	# analyze codes ending with .py

format:	
	black mylib/*.py 

test:
	python3 -m pytest -vv test/test_*.py
	
		
all: install lint format test 