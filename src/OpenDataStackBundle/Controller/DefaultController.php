<?php

namespace OpenDataStackBundle\Controller;

use OpenDataStackBundle\Helper\KibanaHelper;
use OpenDataStackBundle\Helper\LogHelper;

use Elasticsearch\ClientBuilder;
use Enqueue\Fs\FsConnectionFactory;
use Nelmio\ApiDocBundle\Annotation\ApiDoc;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Method;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Route;

use Symfony\Bundle\FrameworkBundle\Controller\Controller;
use Symfony\Component\Filesystem\Exception\IOException;

use Symfony\Component\Finder\Finder;
use Symfony\Component\HttpFoundation\JsonResponse;

use Symfony\Component\HttpFoundation\Request;
use GuzzleHttp\Client;


class DefaultController extends Controller
{
    /**
     * @var Client $http
     */
    private $http;

    /**
     *
     * @return array DefaultController::jsonResponse() arguments.
     */
    private function _createImportConfiguration($payload) {
        $logstack = array();
        $jsonResponse = array();

        $udid = $payload->id;
        $mappings = $payload->config->mappings;
        $templateName = 'dkan-' . $udid;
        $templateNameDashStar = $templateName . '-*';

        // For writing config and log files.
        $fs = $this->container->get('filesystem');

        // 3. Add mapping template to Elasticsearch.
        // 3.1. init Elasticsearch client.
        $client = $this->getClientBuilder();

        // (re)Create Template mapping for indexes under this import
        // configuration.
        if ($client->indices()->existsTemplate(['name' => $templateName])) {
            $clientDeleteTemplateResponse = $client->indices()->deleteTemplate(['name' => $templateName]);

            $logstack[] = LogHelper::prepareLog("previous {$templateName} mapping template found and deleted",
                $clientDeleteTemplateResponse);
        }

        $clientPutTemplateResponse = $client->indices()->putTemplate([
            'name' => $templateName,
            'body' => [
                'index_patterns' => [$templateName . '-*'],
                'settings' => ['number_of_shards' => 1],
                'mappings' => $mappings
            ]
        ]);

        // We have the mapping in ES by this time, save the config and add log.
        LogHelper::persisteJson($fs, "{$udid}", $payload, 'config.json');
        $logstack[] = LogHelper::prepareLog("dataset {$udid} created.", $clientPutTemplateResponse);

        // Create minimal kibana index pattern. The fields will be updated
        // during the data upload.
        $updateLogs = array();
        $message = "Created Kibana {$templateNameDashStar} index-pattern.";
        $status = 200;
        try {
            KibanaHelper::kibanaUpsertIndexPattern($client, $templateNameDashStar,
                $templateNameDashStar, array(), $updateLogs);
        } catch (\Exception $e) {
            // Make sure to log the message.
            $status = 500;
            $message ="Failed to create Kibana {$templateNameDashStar} index-pattern.";
        }
        finally {
            // Get the status of the ES request.
            $status = !empty($updateLogs['status']) ? $updateLogs['status'] : $status;

            // Save the log stack.
            $logstack[] = LogHelper::prepareLog($message, $updateLogs);
            LogHelper::persisteJson($fs, "{$udid}", $logstack, 'log.json');

            return array($status, $message, $updateLogs);
        }
    }

    /**
     * Add Import Configuration.
     *
     * @Route("/import-configuration")
     * @Method("POST")
     * @ApiDoc(
     *   tags={"in-development"},
     *   description="Add a new Import to Elasticsearch-Importer",
     *   method="POST",
     *   section="Import Configurations",
     *   statusCodes={
     *       200="success",
     *       400="error",
     *   },
     * )
     */
    public function addImportConfigurationAction(Request $request)
    {
        $logstack = [];
        $payloadJson = $request->getContent();

        // 1. Request & Data Validation

        if (!$payloadJson) {
            return $this->jsonResponse(400, "empty config parameters");
        }

        $payload = json_decode($payloadJson);

        if (json_last_error() != JSON_ERROR_NONE) {
            return $this->jsonResponse(400, json_last_error_msg());
        }

        if (!property_exists($payload, "id")
            || !property_exists($payload, "type")
            || !property_exists($payload, "config")
        ) {
            return $this->jsonResponse(400, "Missing keys");
        }

        // For writing config and log files.
        $fs = $this->container->get('filesystem');

        if ($fs->exists("/tmp/importer/configurations/" . $payload->id)) {
            return $this->jsonResponse(400, "Import configuration exist already", array());
        }

        list($status, $message, $info) = $this->_createImportConfiguration($payload);

        return $this->jsonResponse($status, $message, $info);
    }

    /**
     * Update Import Configuration
     * @Route("/import-configuration")
     * @Method("PUT")
     * @ApiDoc(
     *   tags={"in-development"},
     *   description="Update an Import Configuration",
     *   method="PUT",
     *   section="Import Configurations",
     *   statusCodes={
     *       200="success",
     *       400="error",
     *   },
     * )
     */
    public function updateImportConfigurationAction(Request $request)
    {
        $payloadJson = $request->getContent();

        // 1. Request & Data Validation

        if (!$payloadJson) {
            return $this->jsonResponse(400, "empty config parameters");
        }

        $payload = json_decode($payloadJson);

        if (json_last_error() != JSON_ERROR_NONE) {
            return $this->jsonResponse(400, json_last_error_msg());
        }

        if (!property_exists($payload, "id")
            || !property_exists($payload, "type")
            || !property_exists($payload, "resources")
            || !property_exists($payload, "config")
        ) {
            return $this->jsonResponse(400, "Missing keys");
        }

        // Recreate the import configuration folder structure
        $udid = $payload->id;
        $resources = $payload->resources;

        // Get previous logs if any.
        $logJson = "{}";
        if (file_exists("/tmp/importer/configurations/{$udid}/log.json")) {
            $logJson = file_get_contents("/tmp/importer/configurations/{$udid}/log.json");
        }
        // Make sure to decode to an array to be able to update the stack later on.
        $logstack = json_decode($logJson, TRUE);

        // Delete old data.
        $fs = $this->container->get('filesystem');

        try {
            $fs->remove("/tmp/importer/configurations/{$udid}");
        } catch (IOException $exception) {
            return $this->jsonResponse(400, "Failed to delete current {$udid} data.", $exception->getMessage());
        }

        // Add the operation to the logs and restore them to file. Reset the log stack.
        $logstack[] = LogHelper::prepareLog("Deleted old configuration and resources.");
        LogHelper::persisteJson($fs, "{$udid}", $logstack, 'log.json');
        $logstack = array();

        // Create the import configuration.
        list($status, $message, $info) = $this->_createImportConfiguration($payload);

        if ($status != 200) {
            // Creating the import configuration failed. Propagate.
            return $this->jsonResponse($status, $message, $info);
        }

        // Produce messages for the resources provided
        foreach ($resources as $resource) {
           $this->importSingleResource($payload, $resource, $udid);
        }

        $message = "Resources for import configuration {$udid} are queued.";
        $logstack[] = LogHelper::prepareLog($message);
        LogHelper::persisteJson($fs, "{$udid}", $logstack, 'log.json');
        return $this->jsonResponse(200, $message, $logstack);
    }

    /**
     * Queue a single resource.
     */
    private function importSingleResource($payload, $resource, $udid)
    {
        $connectionFactory = new FsConnectionFactory('/tmp/importer/enqueue');
        $context = $connectionFactory->createContext();

        // Prepare data
        $data = [
            'importer' => $payload->type,
            'uri' => $resource->uri,
            'udid' => $udid,
            'id' => $resource->id
        ];

        // Queue data
        $queue = $context->createQueue('importQueue');
        $context->createProducer()->send(
            $queue,
            $context->createMessage(json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES))
        );

        // Logs.
        $logstack = [];
        $logstack[] = [
            "message" => "resource {$resource->id} created.",
            "created_at" => date('Y-m-d H:i:s'),
            "status" => "queued"
        ];

        LogHelper::persisteJson($this->container->get('filesystem'), "{$udid}/{$resource->id}", $logstack, 'log.json');
    }

    /**
     * status Resource
     * @Route("/import-configuration/{udid}/resource/{resourceId}")
     * @Method("GET")
     * @ApiDoc(
     *   tags={"in-development"},
     *   description="Status for a csv resource",
     *   method="GET",
     *   requirements={
     *    {
     *      "name"="udid",
     *      "dataType"="string",
     *      "description"="udid of the dataset"
     *    },
     *     {
     *      "name"="resourceId",
     *      "dataType"="string",
     *      "description"="udid of the resource"
     *    }
     *   },
     *   section="Import Configurations",
     *   statusCodes={
     *       200="success",
     *       404="not found",
     *       400="error",
     *   },
     * )
     */
    public function statusResourceAction($udid, $resourceId)
    {

        if (!file_exists("/tmp/importer/configurations/{$udid}")) {
            return $this->jsonResponse(404, "no configuration with the udid: {$udid}");
        }

        if (!file_exists("/tmp/importer/configurations/{$udid}/{$resourceId}")) {
            return $this->jsonResponse(404, "no resource with the udid: {$resourceId}");
        }

        // parse log file and return the persisted status
        $logJson = file_get_contents("/tmp/importer/configurations/{$udid}/{$resourceId}/log.json");
        $log = json_decode($logJson);

        return $this->jsonResponse(200, $log->message, ["flag" => $log->status]);
    }


    /**
     * status Configuration
     * @Route("/import-configuration/{udid}/resources")
     * @Method("GET")
     * @ApiDoc(
     *   description="Returns list resources for an Import configuration",
     *   tags={"in-development"},
     *   method="GET",
     *   requirements={
     *    {
     *      "name"="udid",
     *      "dataType"="string",
     *      "description"="udid of the dataset"
     *    }
     *   },
     *   section="Import Configurations",
     *   statusCodes={
     *       200="success",
     *       400="error",
     *       404="not found",
     *   }
     *
     * )
     */
    public function statusResourcesListAction($udid)
    {

        // Get list of folders for the import configurations
        $finder = new Finder();
        $folders = $finder->directories()->in("/tmp/importer/configurations/{$udid}");

        $listResources = [];
        foreach ($folders as $folder) {
            $listResources[] = basename($folder);
        }

        // Response with list of saved import configurations
        $status = 0;
        $message = "";
        if ($listResources) {
            $status = 200;
            $message = vsprintf("%d resource(s) found", count($listResources));
        } else {
            $status = 404;
            $message = "No result";
        }
        $status = ($listResources) ? 200 : 404;
        return $this->jsonResponse($status, $message, ["ids" => $listResources]);

    }

    /**
     * status Configuration
     * @Route("/import-configuration/{udid}")
     * @Method("GET")
     * @ApiDoc(
     *   description="Returns a status for an Import configuration",
     *   tags={"in-development"},
     *   method="GET",
     *   requirements={
     *    {
     *      "name"="udid",
     *      "dataType"="string",
     *      "description"="udid of the resource"
     *    }
     *   },
     *   section="Import Configurations",
     *   statusCodes={
     *       200="success",
     *       400="error",
     *       404="not found",
     *   }
     *
     * )
     */
    public function statusConfigurationAction($udid)
    {

        if (!file_exists("/tmp/importer/configurations/{$udid}")) {
            return $this->jsonResponse(404, "no configuration with the udid: {$udid}");
        }

        // parse log file and return the persisted status
        $logJson = file_get_contents("/tmp/importer/configurations/{$udid}/log.json");
        $log = json_decode($logJson);

        return $this->jsonResponse(200, $log->message);
    }

    /**
     * status Configurations List
     * @Route("/import-configurations")
     * @Method("GET")
     * @ApiDoc(
     *   description="Returns a list of Import configurations",
     *   tags={"in-development"},
     *   method="GET",
     *   section="Import Configurations",
     *   statusCodes={
     *       200="success",
     *       400="error",
     *       404="not found",
     *   }
     *
     * )
     */
    public function statusConfigurationsListAction()
    {

        // Get list of folders for the import configurations
        $finder = new Finder();
        $folders = $finder->directories()->in("/tmp/importer/configurations");

        $listImportConfigurations = [];
        foreach ($folders as $folder) {
            $listImportConfigurations[] = basename($folder);
        }

        // Response with list of saved import configurations
        $status = 0;
        $message = "";
        if ($listImportConfigurations) {
            $status = 200;
            $message = vsprintf("%d import configurations found", count($listImportConfigurations));
        } else {
            $status = 404;
            $message = "No result";
        }
        $status = ($listImportConfigurations) ? 200 : 404;
        return $this->jsonResponse($status, $message, ["ids" => $listImportConfigurations]);
    }

    /**
     * delete Configuration
     * @Route("/import-configuration/{uuid}")
     * @Method("DELETE")
     * @ApiDoc(
     *   description="Delete an Import configuration",
     *   tags={"in-development"},
     *   method="DELETE",
     *   requirements={
     *    {
     *      "name"="uuid",
     *      "dataType"="string",
     *      "description"="uuid of the resource to be deleted"
     *    }
     *   },
     *   section="Import Configurations",
     *   statusCodes={
     *       200="success",
     *       400="error",
     *       404="not found",
     *   }
     * )
     */
    public function deleteConfigurationAction($uuid)
    {
        if (!$uuid) {
            return $this->jsonResponse(400, "uuid parameters required");
        }

        if (!file_exists("/tmp/importer/configurations/{$uuid}")) {
            return $this->jsonResponse(404, "no configuration with the uuid: {$uuid}");
        }

        // Remove the import configurations folder, template and related indexes
        $fs = $this->container->get('filesystem');

        try {
            $client = $this->getClientBuilder();
            $this->indexTemplateDeleteAction($uuid, $client);
            $fs->remove("/tmp/importer/configurations/{$uuid}");
        } catch (\Exception $exception) {
            return $this->jsonResponse(400, $exception->getMessage());
        }

        return $this->jsonResponse(200, "The dataset with the uuid: {$uuid} has been deleted");
    }

    /**
     * Delete a dataset template and its indexes from Elasticsearch and Kibana
     */
    private function indexTemplateDeleteAction($uuid, $client)
    {
        // Delete dataset template
        $templateName = 'dkan-' . $uuid;
        if ($client->indices()->existsTemplate(['name' => $templateName])) {
            $client->indices()->deleteTemplate(['name' => $templateName]);
        }
        // Fix the $indexName by adding regex
        $indexName = 'dkan-' . $uuid . '-*';
        if ($client->indices()->exists(['index' => $indexName])) {
            $client->indices()->delete(['index' => $indexName]);
        }

        /*
         * Delete index patterns from kibana.
         */
        // Get all current Kibana users indices.
        $kibana_indices = $client->cat()->indices(array('index' => '.kibana*',));
        // Init loop variables.
        $kibana_indexpattern_id = $templateName . '-*';
        $bulk_params = array('body' => array());

        foreach ($kibana_indices as $kibana_index) {
            $bulk_params['body'][] = array(
                'delete' => array(
                    '_index' => $kibana_index['index'],
                    '_type' => 'doc',
                    '_id' => 'index-pattern:' . $kibana_indexpattern_id,
                )
            );
        }

        $client->bulk($bulk_params);
    }

    /**
     * Create a new instance of ClientBuilder
     */
    private function getClientBuilder()
    {
        $client = ClientBuilder::create()
            ->setHosts([$this->container->getParameter('elastic_server_host')])
            ->setSSLVerification(false)
            ->build();
        return $client;
    }

    /**
     * Delete Resource
     * @Route("/request-import/{uuid}/resource/{resourceId}")
     * @Method("DELETE")
     * @ApiDoc(
     *   description="Delete a resource",
     *   tags={"in-development"},
     *   method="DELETE",
     *   requirements={
     *    {
     *      "name"="uuid",
     *      "dataType"="string",
     *      "description"="uuid of the resource dataset"
     *    },
     *    {
     *      "name"="resourceId",
     *      "dataType"="string",
     *      "description"="resourceId of the resource to be deleted"
     *    }
     *   },
     *   section="Import Configurations",
     *   statusCodes={
     *       200="success",
     *       400="error",
     *       404="not found",
     *   }
     *
     * )
     */
    public function requestClearAction($uuid, $resourceId)
    {
        if (!$uuid || !$resourceId) {
            return $this->jsonResponse(400, "uuid and resourceId parameters are required!");
        }

        if (!file_exists("/tmp/importer/configurations/{$uuid}")) {
            return $this->jsonResponse(404, "No configuration with the uuid: {$uuid}");
        }

        if (!file_exists("/tmp/importer/configurations/{$uuid}/{$resourceId}")) {
            return $this->jsonResponse(404, "No resource with the uuid: {$resourceId}");
        }

        // Remove the resource folder
        $fs = $this->container->get('filesystem');
        try {
            $fs->remove("/tmp/importer/configurations/{$uuid}/{$resourceId}");
            $client = $this->getClientBuilder();
            $indexName = 'dkan-' . $uuid . '-' . $resourceId;
            if ($client->indices()->exists(['index' => $indexName])) {
                $client->indices()->delete(['index' => $indexName]);
            }
        } catch (\Exception $exception) {
            return $this->jsonResponse(400, $exception->getMessage());
        }

        return $this->jsonResponse(200, "The resource with the {$resourceId} has been deleted");
    }

    /**
     * request ImportConfiguration
     * @Route("/request-import")
     * @Method("POST")
     * @ApiDoc(
     *   description="Request a resource fetch",
     *   tags={"in-development"},
     *   method="PUT",
     *   section="Import Configurations",
     *   statusCodes={
     *       200="success",
     *       400="error",
     *       404="not found",
     *   }
     *
     * )
     */
    public function requestImportConfigurationAction(Request $request)
    {

        // 1. Validate json payload content

        $payloadJson = $request->getContent();

        if (!$payloadJson) {
            return $this->jsonResponse(400, "empty config parameters");
        }

        $payload = json_decode($payloadJson);

        if (json_last_error() != JSON_ERROR_NONE) {
            return $this->jsonResponse(400, json_last_error_msg());
        }

        if (!property_exists($payload, "id") || !property_exists($payload, "type")
            || !property_exists($payload, "url") || !property_exists($payload, "udid")) {
            return $this->jsonResponse(400, "Missing keys");
        }

        $udid = $payload->udid;
        $resourceId = $payload->id;

        if (!file_exists("/tmp/importer/configurations/{$udid}")) {
            return $this->jsonResponse(404, "no configuration with the udid: {$udid}");
        }

        // 2. Update import config status to : ** queued **

        $date = new \DateTime('now');
        $timestamp = $date->format('Y-m-d H:i:s');
        $log = [
            "message" => "resource {$resourceId} created at {$timestamp}",
            "created_at" => $timestamp,
            "status" => "queued"
        ];

        $logJson = json_encode($log);

        // Make sure the destination directory exidts.
        $fs = $this->container->get('filesystem');
        $dest = "/tmp/importer/configurations/{$udid}/{$resourceId}";
        $fs->mkdir($dest, 0777, TRUE);
        file_put_contents("{$dest}/log.json", $logJson);

        // 3. Produce a message to process in the queue

        chown("/tmp/importer/enqueue", "www-data");
        $connectionFactory = new FsConnectionFactory('/tmp/importer/enqueue');
        $context = $connectionFactory->createContext();

        $data = [
            'importer' => $payload->type,
            'uri' => $payload->url,
            'udid' => $payload->udid,
            'id' => $payload->id
        ];
        $queue = $context->createQueue('importQueue');
        $context->createProducer()->send(
            $queue,
            $context->createMessage(json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES))
        );

        return $this->jsonResponse(200, "Resource {$resourceId} for import configuration {$udid} is queued");
    }

    /**
     * A Json Response helpers
     *
     * @param $status 200:success , 40* for failed
     * @param $message
     * @param array|NULL $info
     *
     * @return \Symfony\Component\HttpFoundation\JsonResponse
     */
    private function jsonResponse($status, $message, array $info = null)
    {
        $response = new JsonResponse(
            [
                "status" => ($status == 200 ? "success" : "fail"),
                "message" => $message,
                "info" => $info
            ],
            $status
        );
        return $response;
    }
}
