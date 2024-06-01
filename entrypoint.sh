docker build -t task-1 .
docker run --name xml-csv-parser -it -v $(pwd)/input:/app/input -v $(pwd)/output:/app/output task-1
