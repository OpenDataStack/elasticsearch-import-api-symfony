<?php

namespace OpenDataStackBundle\Controller;

use Nelmio\ApiDocBundle\Annotation\ApiDoc;
use Symfony\Bundle\FrameworkBundle\Controller\Controller;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Route;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Method;
use Enqueue\Fs\FsConnectionFactory;
use Symfony\Component\Filesystem\Exception\IOException;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;

class DefaultController extends Controller
{
    /**
     * @Route("/test")
     */
     public function indexAction()
     {
       // TODO Shift to services
       $connectionFactory = new FsConnectionFactory('/tmp/enqueue');
       $context = $connectionFactory->createContext();

       $data = [
         'importer' => 'opendatastack/csv',
         'uri' => 'https://datos.colombiacompra.gov.co/csvdata/2013/20136.csv'
       ];
       $queue = $context->createQueue('importQueue');
       $context->createProducer()->send(
         $queue,
         $context->createMessage(json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES))
       );

       // Example of passing environment variables from docker
       $elastic_server = $this->getParameter('elastic_server_host');
       return $this->render('OpenDataStackBundle:Default:index.html.twig',
         [
           'message' => 'Hello there',
           'elastic_server' => $elastic_server,
         ]
       );
     }


     /**
     * @Route("/ping")
     * @Method("POST")
     */
     public function pingAction(Request $request)
     {

       $data = $request->request->get('data', 'response ok : send a message in a data key');

       $response = new JsonResponse($data, 200);

       return $response;
     }


    /**
     * @Route("/import-configuration")
     * @Method("POST")
     * @ApiDoc(
     *   tags={"in-development"},
     *   description="Add a new Import to Elasticsearch-Importer",
     *   method="POST",
     *   section="Import Configurations",
     *   parameters={
     *   {"name"="config", "dataType"="string", "format"="json", "required"=true, "description"="resource info and mapping data"}
     *   },
     *   statusCodes={
     *       200="success",
     *       400="error",
     *   },
     * )
     */
    public function addImportConfigurationAction(Request $request)
    {
      $payloadJson = $request->getContent();

      if (!$payloadJson) {
        $response = new JsonResponse(
          array("log" => array("status" => "fail", "message" => "empty config parameters")),
          400);
        return $response;
      }

      $payload = json_decode($payloadJson);
      if (json_last_error() != JSON_ERROR_NONE) {
        $response = new JsonResponse(
          array("log" => array("status" => "fail", "message" => json_last_error_msg())),
          400);
        return $response;
      }

      if (!property_exists($payload, "id") || !property_exists($payload, "type") || !property_exists($payload, "config")) {
        $response = new JsonResponse(
          array("log" => array("status" => "fail", "message" => "Missing keys")),
          400);
        return $response;
      }

      $udid = $payload->id;
      $fs = $this->container->get('filesystem');

      if ($fs->exists("/tmp/{$udid}")) {
        $response = new JsonResponse(
          array("log" => array("status" => "fail", "message" => "Import configuration exist already")),
          400);
        return $response;
      }

      // Persist import-configuration in the filesystem
      try {
        $fs->mkdir("/tmp/{$udid}");
      } catch (IOException $exception) {
        $response = new JsonResponse(
          array("log" => array("status" => "fail", "message" => "Folder creation error")),
          400);
        return $response;
      }

      $date = new \DateTime('now');
      $timestamp = $date->format('Y-m-d H:i:s');
      $log = array("status" => "new", "message" => "{$udid} created at {$timestamp}", "created_at" => $timestamp);

      $logJson = json_encode($log);
      file_put_contents("/tmp/{$udid}/log.json", $logJson);
      file_put_contents("/tmp/{$udid}/config.json", $payloadJson);

      $response = new JsonResponse(
        array("log" => array("status" => "success", "message" => $log['message'], "flag" => $log['status'])),
        200);
      return $response;

    }


    /**
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
    public function statusConfigurationAction($udid) {

      if (!$udid){
        $response = new JsonResponse(
          array("log" => array("status" => "fail", "message" => "udid parameters required")),
          400);
        return $response;
      }

      if (!file_exists("/tmp/{$udid}")) {
        $response = new JsonResponse(
          array("log" => array("status" => "fail", "message" => "no configuration with the udid: {$udid}")),
          404);
        return $response;
      }

      $logJson = file_get_contents("/tmp/{$udid}/log.json");
      $log = json_decode($logJson);

      $response = new JsonResponse(
        array("log" => array("status" => "success", "message" => $log->message, "flag" => $log->status)),
        200);
      return $response;

    }

  /**
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
  public function statusConfigurationsListAction() {

    }

    /**
     * @Route("/debug")
     */
      public function debugAction() {

        $message = "folder /tmp/photos does not exist";

        $fs = $this->container->get('filesystem');
        //$fs->mkdir("/tmp/photos");

        if ($fs->exists("/tmp/photos")) {
          $message = "folder /tmp/photos exist";
        }

        return $this->render('OpenDataStackBundle:Default:debug.html.twig',
          ['message' => $message]
        );
      }
         
}
