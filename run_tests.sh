docker build -f Dockerfile.test -t test_app .
docker run --name test-xml-csv-parser -d test_app
