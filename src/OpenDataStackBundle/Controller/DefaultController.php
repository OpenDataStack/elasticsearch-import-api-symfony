<?php

namespace OpenDataStackBundle\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\Controller;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Route;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Method;
use Enqueue\Fs\FsConnectionFactory;
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
}
