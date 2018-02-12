<?php

namespace OpenDataStackBundle\Command;

use OpenDataStackBundle\Helper\KibanaHelper;

use Elasticsearch\ClientBuilder;
use Enqueue\Consumption\QueueConsumer;
use Enqueue\Fs\FsConnectionFactory;
use Interop\Queue\PsrMessage;

use Interop\Queue\PsrProcessor;
use League\Csv\Reader;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputInterface;

use Symfony\Component\Console\Output\OutputInterface;

class ImportCommand extends ContainerAwareCommand
{
    protected function configure()
    {
        $this
            ->setName('ods:import')
            ->setDescription('Process the import file queue');
    }

    /**
     * Handle requests to import dkan resources and prepare them for indexing
     *
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $output->writeln("Listening for the Queue: --importQueue--");

        // Initialise a Queue consumer
        $connectionFactory = new FsConnectionFactory('/tmp/importer/enqueue');
        //TODO: chown '/tmp/importer/enqueue' to 'www-data'
        $context = $connectionFactory->createContext();
        $importQueue = $context->createQueue('importQueue');
        $queueConsumer = new QueueConsumer($context);

        // Initialise Elasticsearch PHP Client
        $client = ClientBuilder::create()
            ->setHosts([$this->getContainer()->getParameter('elastic_server_host')])
            ->setLogger(ClientBuilder::defaultLogger('/tmp/importer/importer.log'))
            ->setSSLVerification(false)
            ->build();

        // Anonymous block function to handle incoming requests
        $queueConsumer->bind('importQueue', function (PsrMessage $message) use (&$output, $client) {
            $output->writeln("Processing Job Import");

            // Parse payload from the REST call
            $data = json_decode($message->getBody(), true);
            $udid = $data['udid'];
            $resourceId = $data['id'];

            $configJson = file_get_contents("/tmp/importer/configurations/{$udid}/config.json");
            $config = json_decode($configJson, true);

            $mapping = $config['config']['mappings'];
            reset($mapping);
            $indexType = key($mapping);

            $output->writeln("Import Type: " . $data['importer']);
            $output->writeln("URI: " . $data['uri']);

            // Update import config status to : ** importing **
            $log = null;
            if (file_exists("/tmp/importer/configurations/{$udid}/{$resourceId}/log.json")) {
                $logJson = file_get_contents("/tmp/importer/configurations/{$udid}/{$resourceId}/log.json");
                $log = json_decode($logJson);
            } else {
                $log = new \stdClass;
            }

            $log->status = "importing";
            $logJson = json_encode($log);
            file_put_contents("/tmp/importer/configurations/{$udid}/{$resourceId}/log.json", $logJson);

            // 1. Download CSV Resource
            $uniqueFileName = vsprintf("resource_%s.csv", uniqid());
            $filePath = "/tmp/importer/configurations/" . $udid . "/" . $resourceId . "/" . $uniqueFileName;
            $uri = $data['uri'];

            // if the download fails , update log status to **error** and remove the message from the queue
            if (!file_put_contents($filePath, fopen($uri, 'r'))) {
                $log->status = "error";
                $logJson = json_encode($log);
                file_put_contents("/tmp/importer/configurations/{$udid}/{$resourceId}/log.json", $logJson);

                return PsrProcessor::REJECT;
            }

            $output->writeln("Resource for dataset({$udid}) downloaded successfully");

            // 2. Parse CSV Resource
            $csv = Reader::createFromPath($filePath, 'r');
            $csv->setHeaderOffset(0);

            $header = $csv->getHeader(); //returns the CSV header record
            $records = $csv->getRecords(); //returns all the CSV records as an Iterator object

            // 3. Clear & Recreate index
            $index = 'dkan-' . $udid . '-' . $resourceId;
            if ($client->indices()->exists(['index' => $index])) {
                $client->indices()->delete(['index' => $index]);
            }
            $response = $client->indices()->create(['index' => $index]);

            // 4. Batch index 1000 records at a time
            foreach ($records as $key => $rowFields) {
                $params['body'][] = [
                    'index' => [
                        '_index' => $index,
                        '_type' => $indexType
                    ]
                ];

                $params['body'][] = $rowFields;

                // Every 1000 documents stop and send the bulk request
                if ($key % 1000 == 0) {
                    $response = $client->bulk($params);
                    $params = ['body' => []];
                }
            }

            // Send the last batch
            if (!empty($params['body'])) {
                $response = $client->bulk($params);
            }

            // The Kibana index-pattern was created in the HTTP request but
            // with empty settings. Update the index-pattern fields.
            $updateLogs = array();
            $index_pattern_fields = KibanaHelper::kibanaGetFieldMapping($client, $index);

            // Update Kibana index patterns fields mapping for all of the
            // .kibana indices.
            KibanaHelper::kibanaUpsertIndexPattern($client, 'dkan-' . $udid . '-*',
                'dkan-' . $udid . '-*', $index_pattern_fields, $updateLogs);

            // Update the Full "dkan-*" kibana index-pattern.
            $index_pattern_fields = KibanaHelper::kibanaGetFieldMapping($client);
            KibanaHelper::kibanaUpsertIndexPattern($client, 'dkan-*', 'dkan-*',
                $index_pattern_fields, $updateLogs);

            // 5. Update import config status to : ** importing **
            $logJson = file_get_contents("/tmp/importer/configurations/{$udid}/{$resourceId}/log.json");
            $log = json_decode($logJson);
            $log->status = "done";
            $logJson = json_encode($log);
            file_put_contents("/tmp/importer/configurations/{$udid}/{$resourceId}/log.json", $logJson);

            return PsrProcessor::ACK;
        });

        $queueConsumer->consume();
    }
}
