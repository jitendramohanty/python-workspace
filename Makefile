install:
	pip install -r requirements.txt
lint:
	pylint mymodule.py
format:
	black mymodule.py