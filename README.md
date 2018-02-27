# Summary

A Symfony application designed to handle dataset resources import and index to elasticsearch. The application
have a REST Api endpoint and a message queue based on filesystem. 

## How it works

For each dataset, a new folder is created holding the data schema and the status log. 
When a request to import is made to the API, a resource uri is add to the queue in order to be fetched, parsed and indexed to elasticsearch


## Usage
The homepage of the project shows the API documentation made by Nelmio bundle. 
You can see the Rest api methods and a useful sandbox foreach method to test it

**Running**

to run the project using Php internal server, run the following command:

```bash
> php bin/console server:start
```
and open the url : [localhost:8000](http://localhost:8000)
#### Endpoints

- **Model**
  - [POST] import-configuration
  - [GET] import-configuration/$id
  - [GET] import-configurations
  - [DELETE] import-configuration/$id

- **Import Processing**
  - [POST] import-request

## Development & Test Cycle

#### Add an import configuration to register an entrypoint for any resources

- example : 
```rest
POST /import-configurations
```
See example file config.json for the request body

> src/OpenDataStackBundle/Controller/examples/config.json


#### Run the Queue consumer command
To launch the queue listener for incoming request to import , run the following symfony command from the root of the project :

```bash
> php bin/console ods:import
```

#### Post request to [/request-import](http://localhost:8000/request-import)
- example : 
```rest
POST /request-import
{
  "udid": "123-dataset-123", // Dataset udid
  "id": "resource-abc",      // Resource udid
  "type": "opendatastack/csv-importer",
  "url": "https://cdn.rawgit.com/achoura/elkd/aa0074ac/model_slug.csv"
}
```

## Coding Standards
The project is in compliance to PSR-1 and PSR-2 for code style , and PSR-4 for autoloading

To apply the php standards for your code, add the php-cs-fixer utility :

```bash
> composer global require friendsofphp/php-cs-fixer
```

Then from the root of your project , run :

```bash
> php-cs-fixer fix
```

## Use in Docker

The project include a Dockerfile for simple Symfony app container

- Based on ubuntu:16.04 for developer simplicity (not size)
- With nginx + php-fpm
- With rabbit-mq via php-enqueue/ampq
- Custom nginx, php and supervisord configurations
- Without a database

## Usage

### Variables

- $WWW_UID and $WWW_GID : Optional to change the UID/GID that nginx runs under to
solve permission issues if you are mounting a directory from your host

### Running

Pull from docker hub and run:

```
docker pull opendatastack/elasticsearch-import-api
docker run \
-e "APP_ELASTIC_SERVER_HOST=http://anotherurl:9400" \
-p 85:80 \
--name elasticsearch-import-api opendatastack/elasticsearch-import-api;
```

## Development & Test Cycle

Download: ```git clone git@github.com:OpenDataStack/docker-elasticsearch-import-api.git && cd docker-elasticsearch-import-api```

Change:

```
docker pull opendatastack/docker-symfony

```

```
docker rm elasticsearch-import-api;
docker build -t elasticsearch-import-api .;
docker run \
-e "APP_ELASTIC_SERVER_HOST=http://anotherurl:9400" \
-p 85:80 \
--name elasticsearch-import-api elasticsearch-import-api:latest;
```

Commit and push:

```
docker login
docker tag elasticsearch-import-api opendatastack/elasticsearch-import-api
docker push opendatastack/elasticsearch-import-api
```

### Debugging

Login:

```
docker exec -it elasticsearch-import-api /bin/bash
```

Copy files from the container:

```
docker cp elasticsearch-import-api:/etc/php/7.0/fpm/php.ini /PATH/TO/FILE
```
