<?php

namespace OpenDataStackBundle\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\Controller;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Route;

class DefaultController extends Controller
{
    /**
     * @Route("/test")
     */
    public function indexAction()
    {
      //$elastic_server = $this->getParameter('elastic_server_host');
      $elastic_server = "Hi";
      //exit();
      return $this->render('OpenDataStackBundle:Default:index.html.twig',
        [
          'message' => 'Hello there',
          'elastic_server' => $elastic_server,
        ]
      );
    }
}
