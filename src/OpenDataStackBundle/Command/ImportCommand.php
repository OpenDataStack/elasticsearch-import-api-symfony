<?php

namespace OpenDataStackBundle\Command;

use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

use Interop\Queue\PsrMessage;
use Interop\Queue\PsrProcessor;
use Enqueue\Fs\FsConnectionFactory;
use Enqueue\Consumption\QueueConsumer;

class ImportCommand extends ContainerAwareCommand
{
    protected function configure()
    {
        $this
            ->setName('ods:import')
            ->setDescription('Process the import file queue')
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $output->writeln("Starting import processor...");

        // TODO: Shift to services
        $connectionFactory = new FsConnectionFactory('/tmp/queue');
        $context = $connectionFactory->createContext();
        $importQueue = $context->createQueue('importQueue');
        $queueConsumer = new QueueConsumer($context);

        $queueConsumer->bind('importQueue', function(PsrMessage $message) use (&$output) {
          $output->writeln("Processing Job...");
          $data = json_decode($message->getBody(), true);
          $output->writeln("Import Type: " . $data['importer']);
          $output->writeln("URI: " . $data['uri']);
          return PsrProcessor::ACK;
        });

        $queueConsumer->consume();
    }
}
