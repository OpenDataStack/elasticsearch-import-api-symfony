<?php

namespace OpenDataStackBundle\Command;

use Elasticsearch\ClientBuilder;
use Elasticsearch\Common\Exceptions\BadRequest400Exception;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

use Interop\Queue\PsrMessage;
use Interop\Queue\PsrProcessor;
use Enqueue\Fs\FsConnectionFactory;
use Enqueue\Consumption\QueueConsumer;

use Monolog\Logger;

use League\Csv\Reader;


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

        $logger = ClientBuilder::defaultLogger('/tmp/importer.log');

        $client = ClientBuilder::create()
            ->setHosts(['http://elasticsearch:9200'])
            ->setLogger($logger)
            ->setSSLVerification(false)
            ->build();


        $queueConsumer->bind('importQueue', function (PsrMessage $message) use (&$output, $client) {
            // update log file
            $output->writeln("Processing Job Import");

            $data = json_decode($message->getBody(), TRUE);
            $output->writeln("Import Type: " . $data['importer']);
            $output->writeln("URI: " . $data['uri']);


            $uniqueFileName = vsprintf("resource_%s.csv", uniqid());

            // 1. Download CSV Resource
            $filePath = "/tmp/configurations/" . $data['udid'] . "/" . $uniqueFileName;
            $uri = $data['uri'];
            $this->downloadFile($uri, $filePath);

            $udid = $data['udid'];

            $output->writeln("Resource for dataset({$udid}) downloaded successfully");

            // 2. Parse CSV Resource

            $csv = Reader::createFromPath($filePath, 'r');
            $csv->setHeaderOffset(0);

            $header = $csv->getHeader(); //returns the CSV header record
            $records = $csv->getRecords(); //returns all the CSV records as an Iterator object


            // 3. Create/Update Index

            // X. Create/Update Index TEMPLATE (goes in addConfiguration)
/*

            $configJson = file_get_contents("/tmp/configurations/{$udid}/config.json");
            $datasetConfig = json_decode($configJson);

            $mappings = $datasetConfig->config->mappings;
            $elasticIndex = "dkan-".$udid;


            $params = [
                'index' => $elasticIndex,
                'body' => [
                    'settings' => [
                        'number_of_shards' => 3,
                        'number_of_replicas' => 2
                    ],
                    'mappings' => $mappings
                ]
            ];

            $response = $client->indices()->create($params);
*/

            print_r("---------------------");
            die();
            $params = ['body' => []];


            foreach ($records as $key => $rowFields ) {

            // $value : [ocid] => ocds-k50g02-13-10-153724

            //print_r($key);
            //print_r($value);
            //sprintf("key:%s || value:%s", $key, $value);
            //sprintf("header:%s", $header[$key]);

                $params['body'][] = [
                    'index' => [
                        '_index' => $elasticIndex,
                        '_type' => "dkantype"
                    ]
                ];

                // array data
                $params['body'][] = $rowFields;

                // Every 1000 documents stop and send the bulk request
                if ($key % 10 == 0) {
                    $responses = $client->bulk($params);

                    $output->writeln(" -- bulk response -- ");
                    print_r($responses);
                    $output->writeln(" -- end bulk response -- ");

                    // erase the old bulk request
                    $params = ['body' => []];

                    // unset the bulk response when you are done to save memory
                    unset($responses);
                }
            }

            $output->writeln(" -- end foreach -- ");


            // Send the last batch if it exists
            if (!empty($params['body'])) {
                $responses = $client->bulk($params);

                $output->writeln(" -- final bulk response -- ");
                print_r($responses);
                $output->writeln(" -- final bulk response -- ");

            }



            //print_r($header);
            //var_dump(iterator_count($records));

            // test link
            // https://cdn.rawgit.com/achoura/elkd/aa0074ac/model_slug.csv
            // https://cdn.rawgit.com/achoura/elkd/master/model.csv

            // Original
            // https://datos.colombiacompra.gov.co/csvdata/2013/20135.csv


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

        $opts = array(
            "ssl" => array(
                "verify_peer" => FALSE,
                "verify_peer_name" => FALSE,
            ),
        );
        $file = fopen($url, 'rb', FALSE, stream_context_create($opts));
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
