install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

lint:
	pylint --disable=R,C mylib/*.py

format:	
	black mylib/*.py 

all: install lint format