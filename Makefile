
VIRTUALENV_PATH=/opt/yan
BIN_PATH=/usr/bin
CONFIG_PATH=/etc


# By default do nothing

all:


# Cleaning targets:

clean: clean-setup clean-build clean-pyc

clean-setup:
	python setup.py clean --all

clean-build:
	rm -rfv build/
	rm -rfv dist/
	rm -rfv .eggs/
	find . -name '*.egg-info' -exec rm -rfv {} +
	find . -name '*.egg' -exec rm -rfv {} +

clean-pyc:
	find . -name '*.pyc' -exec rm -rfv {} +
	find . -name '*.pyo' -exec rm -rfv {} +
	find . -name '*~' -exec rm -rfv {} +
	find . -name '__pycache__' -exec rm -rfv {} +


# Installation target

install: uninstall
	bash ./setupvenv.sh ${VIRTUALENV_PATH}
	cp yan.sh ${BIN_PATH}/yan
	if [ ! -f ${CONFIG_PATH}/yan.yml ]; then cp postgres.yml ${CONFIG_PATH}/yan.yml; fi


# Uninstallation target

uninstall:
	rm -rf ${BIN_PATH}/yan
	rm -rf ${VIRTUALENV_PATH}
