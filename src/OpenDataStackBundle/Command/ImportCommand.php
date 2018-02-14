<?php

namespace OpenDataStackBundle\Command;

use OpenDataStackBundle\Helper\KibanaHelper;
use OpenDataStackBundle\Helper\LogHelper;

use Elasticsearch\ClientBuilder;
use Enqueue\Consumption\QueueConsumer;
use Enqueue\Fs\FsConnectionFactory;
use Interop\Queue\PsrMessage;

use Interop\Queue\PsrProcessor;
use League\Csv\Reader;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Filesystem\Exception\IOExceptionInterface;

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
            $output->writeln("-- import job INIT --");

            // Inits for logging.
            $logstack = array();
            $fs = new Filesystem();

            // Parse payload from the REST call.
            $data = json_decode($message->getBody(), true);
            $udid = $data['udid'];
            $resourceId = $data['id'];

            $configJson = file_get_contents("/tmp/importer/configurations/{$udid}/config.json");
            $config = json_decode($configJson, true);

            $mapping = $config['config']['mappings'];
            reset($mapping);
            $indexType = key($mapping);

            $output->writeln("Import Type: " . $data['importer']);
            $output->writeln("UDID: {$udid}");
            $output->writeln("Resource ID: {$resourceId}");
            $output->writeln("URI: " . $data['uri']);

            // Log.
            $message = "Import process started.";
            $logstack[] = LogHelper::prepareLog($message);
            $output->writeln($message);

            // Download CSV Resource
            $uniqueFileName = vsprintf("resource_%s.csv", uniqid());
            $filePath = "/tmp/importer/configurations/" . $udid . "/" . $resourceId . "/" . $uniqueFileName;
            $uri = $data['uri'];

            // If the download fails , update log status to **error** and
            // remove the message from the queue.
            if (!file_put_contents($filePath, fopen($uri, 'r'))) {
                $message = "Failed to download the CSV file. Aborting.";
                $logstack[] = LogHelper::prepareLog($message);
                $output->writeln($message);

                return PsrProcessor::REJECT;
            }

            $message = "Resource {$resourceId} for dataset({$udid}) downloaded successfully";
            $logstack[] = LogHelper::prepareLog($message);
            $output->writeln($message);

            // Parse CSV Resource.
            $csv = Reader::createFromPath($filePath, 'r');
            $csv->setHeaderOffset(0);

            $header = $csv->getHeader(); //returns the CSV header record
            $records = $csv->getRecords(); //returns all the CSV records as an Iterator object

            // Clear & Recreate index.
            $index = 'dkan-' . $udid . '-' . $resourceId;
            if ($client->indices()->exists(['index' => $index])) {
                $client->indices()->delete(['index' => $index]);
            }
            $response = $client->indices()->create(['index' => $index]);

            $message = "Elasticsearch index cleared and recreated.";
            $logstack[] = LogHelper::prepareLog($message);
            $output->writeln($message);

            // Batch index 1000 records at a time.
            foreach ($records as $key => $rowFields) {
                $params['body'][] = [
                    'index' => [
                        '_index' => $index,
                        '_type' => $indexType
                    ]
                ];

                $params['body'][] = $rowFields;

                // Every 1000 documents stop and send the bulk request.
                if ($key % 1000 == 0) {
                    $response = $client->bulk($params);
                    $params = ['body' => []];
                }
            }

            // Send the last batch.
            if (!empty($params['body'])) {
                $response = $client->bulk($params);
            }

            $message = "CSV file indexed.";
            $logstack[] = LogHelper::prepareLog($message);
            $output->writeln($message);

            // The Kibana index-pattern was created in the HTTP request but
            // with empty settings. Update the index-pattern fields.
            $kibanaIndexPatternResource = 'dkan-' . $udid . '-*';
            $kibanaIndexPatternGlobal = 'dkan-*';
            $updateLogs = array();
            $index_pattern_fields = KibanaHelper::kibanaGetFieldMapping($client, $kibanaIndexPatternResource);

            // Update Kibana index patterns fields mapping for all of the
            // .kibana indices.
            KibanaHelper::kibanaUpsertIndexPattern($client, $kibanaIndexPatternResource,
                $kibanaIndexPatternResource, $index_pattern_fields, $updateLogs);

            $message = "Kibana '{$kibanaIndexPatternResource}' index-pattern fields updated.";
            $logstack[] = LogHelper::prepareLog($message, $updateLogs);
            $output->writeln($message);

            // Update the Full "dkan-*" kibana index-pattern.
            $index_pattern_fields = KibanaHelper::kibanaGetFieldMapping($client);
            KibanaHelper::kibanaUpsertIndexPattern($client, $kibanaIndexPatternGlobal, $kibanaIndexPatternGlobal,
                $index_pattern_fields, $updateLogs);

            $message = "Kibana '{$kibanaIndexPatternGlobal}' index-pattern fields updated.";
            $logstack[] = LogHelper::prepareLog($message, $updateLogs);
            $output->writeln($message);

            // Last log entry for this job.
            $message = "Import process finished.";
            $logstack[] = LogHelper::prepareLog($message);
            $output->writeln($message);

            // Persiste the logs.
            LogHelper::persisteJson($fs, "{$udid}/{$resourceId}", $logstack, 'log.json');

            $output->writeln("-- import job DONE --");
            return PsrProcessor::ACK;
        });

        $queueConsumer->consume();
    }
}
