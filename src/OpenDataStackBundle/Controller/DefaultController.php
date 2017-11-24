<?php

namespace OpenDataStackBundle\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\Controller;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Route;
use Enqueue\Fs\FsConnectionFactory;

class DefaultController extends Controller
{
    /**
     * @Route("/test")
     */
     public function indexAction()
     {
       // TODO Shift to services
       $connectionFactory = new FsConnectionFactory('/tmp/queue');
       $context = $connectionFactory->createContext();

       $data = [
         'importer' => 'opendatastack/csv',
         'uri' => 'https://datos.colombiacompra.gov.co/csvdata/2013/20136.csv'
       ];
       $context->createProducer()->send(
         $context->createQueue('importQueue'),
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
}
