
all: 

.PHONY: clean test

env: 
	mkdir -p build/
	test -d build/env ||  virtualenv -p python3 build/env
	source build/env/bin/activate ; pip install -r requirements.txt

test: env
	source build/env/bin/activate ; python -m unittest

clean:
	rm -fr build
	find . -type f -name '*~' -delete || true
	find . -type f -name '*.pyc' -delete || true
	find . -type f -name '*.swp' -delete || true
	find . -type d -name '__pycache__' -exec rm -fr '{}' \; || true

