install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

lint:
	pylint --disable=R,C --ignore-patterns=mylib/*.py

format:	
	black mylib/*.py 

all: install lint format