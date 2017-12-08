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

use Goodby\CSV\Import\Standard\Lexer;
use Goodby\CSV\Import\Standard\Interpreter;
use Goodby\CSV\Import\Standard\LexerConfig;


class ImportCommand extends ContainerAwareCommand {
    protected function configure() {
        $this
            ->setName('ods:import')
            ->setDescription('Process the import file queue');
    }

    protected function execute(InputInterface $input, OutputInterface $output) {
        $output->writeln("Listening for the Queue: --importQueue--");

        // TODO: Shift to services
        $connectionFactory = new FsConnectionFactory('/tmp/enqueue');
        $context = $connectionFactory->createContext();
        $importQueue = $context->createQueue('importQueue');
        $queueConsumer = new QueueConsumer($context);

        $queueConsumer->bind('importQueue', function (PsrMessage $message) use (&$output) {
            // update log file
            $output->writeln("Processing Job Import");

            $data = json_decode($message->getBody(), TRUE);
            $output->writeln("Import Type: " . $data['importer']);
            $output->writeln("URI: " . $data['uri']);


            $uniqueFileName = vsprintf("resource_%s.csv", uniqid());

            // 1. Download CSV Resource
            $filePath = "/tmp/configurations/" . $data['udid'] . "/". $uniqueFileName;
            $uri = $data['uri'];
            $this->downloadFile($uri, $filePath);

            $output->writeln("File downloaded successfully");

            // 2. Parse CSV Resource

            $lexer = new Lexer(new LexerConfig());
            $interpreter = new Interpreter();

            $lineNumber = 0;
            $interpreter->addObserver(function(array $row) use (&$lineNumber) {
                $lineNumber += 1;
                print_r($row);

                //TODO: push to elastic when reach batch-size
            });

            $lexer->parse($filePath, $interpreter);

            $output->writeln("*****************************");
            $output->writeln("File parsed");


            // update log file
            // catch any exceptions and update log file with error
            return PsrProcessor::ACK;
        });

        $queueConsumer->consume();
    }

    // helpers
    private function downloadFile($url, $path) {
        $newfname = $path;

        $opts=array(
            "ssl"=>array(
                "verify_peer"=>false,
                "verify_peer_name"=>false,
            ),
        );
        $file = fopen($url, 'rb', false, stream_context_create($opts));
        if ($file) {
            $newf = fopen($newfname, 'wb');
            if ($newf) {
                while (!feof($file)) {
                    fwrite($newf, fread($file, 1024 * 8), 1024 * 8);
                }
            }
        }
        if ($file) {
            fclose($file);
        }
        if ($newf) {
            fclose($newf);
        }
    }
}
