PYTHON = python3

package:
	${PYTHON} setup.py sdist bdist_wheel

clean:
	rm -rf build
	rm -rf dist
	rm -rf *.egg-info

test:
	${PYTHON} -m pytest
