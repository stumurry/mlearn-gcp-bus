# Neo4j Instructions

## Connecting
Log into google cloud `gcloud auth login`
Ensure that the VM is turned on from the following page
https://console.cloud.google.com/compute/instances?project=front-icentris

Once you are logged in set up a tunnel to Neo4j using
```gcloud compute --project "front-icentris" ssh --zone "us-west1-a" "neo4j-1-vm" -- -L 7474:localhost:7474 -L 7687:localhost:7687 -L 8889:localhost:8888```

Once you are connected you can point your web browser to
http://localhost:7474


The password can be found at

https://console.cloud.google.com/compute/instancesDetail/zones/us-west1-a/instances/neo4j-1-vm?project=front-icentris
