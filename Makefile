test:
	python tests.py

verify:
	pyflakes -x W dropboxfs.py
	pep8 --exclude=migrations --ignore=E501,E225 dropboxfs.py

install:
	python setup.py install

publish:
	python setup.py register
	python setup.py sdist upload
