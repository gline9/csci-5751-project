# CSCI 5751 Project

## Connecting to the Server

You can connect using the credentials in the password.kdbx file ([keepass](https://sourceforge.net/projects/keepass/files/KeePass%202.x/2.47/KeePass-2.47-Setup.exe/download) can be used to open the file)

Example connection using ssh

```sh
ssh csuser@csci.gline9.com -p 2322
```

## Data Location

Data is stored in the `./data` directory both in the original zipped `*.json.gz` and unzipped `*.json` files for both the product reviews `review.*` and metadata `metadata.*`. 


## Connecting to HBase


HBase is currently running in a docker container using the `docker-compose.yml` file located in the repo.

* To start it up again you can run the `./startup.sh` script.
* To check if it is running run `docker ps` and make sure a container is running.
* To connect to the instance run `./connect.sh`
* To shutdown the instance run `./shutdown.sh`


The `./data` directory is mounted inside the container at `/json-data` and the database is persisted through a volume so after a shutdown and startup the data will remain.

