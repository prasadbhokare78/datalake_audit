To build

echo "$GITHUB_TOKEN" | docker login ghcr.io -u prasadbhokare78 --password-stdin

docker build -t ghcr.io/prasadbhokare78/audit_pipeline:latest .

docker push ghcr.io/prasadbhokare78/audit_pipeline:latest


To run docker container of the following project 

docker pull ghcr.io/prasadbhokare78/audit_pipeline:latest
docker run -d -p 5000:5000 ghcr.io/prasadbhokare78/audit_pipeline:latest

