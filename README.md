#Summary


A Symfony application designed to handle csv import and index data to elasticsearch. The application
have a REST Api endpoint and a message queue based on filesystem

##How it works

The purpose of this app is to be an importer for datasets resources. for each dataset, a new folder is created holding the data schema and the status log. When a request to import is made to the API, a resource will be queued to be downloaded, parsed and indexed to elasticsearch

##Usage
The homepage of the project shows the API documentation made by Nelmio bundle. 
You can see the Rest api methods and a useful sandbox foreach method to test it

#### Endpoints

- **Model**
  - POST import-configuration
  - GET import-configuration/$id
  - GET import-configurations
  - DELETE import-configuration/$id

- **Import Processing**
  - POST import-request

#### Queue
To launch the queue listener for incoming request to import , run the following symfony command from the root of the project :

```bash
> php bin/console ods:import
```

##Coding Standards
The project is in compliance to PSR-1 and PSR-2 for code style , and PSR-4 for autoloading

To apply the php standards for your code, kindly run this command to reformat the code for you

```bash
> php bin/console project:phpcs
```