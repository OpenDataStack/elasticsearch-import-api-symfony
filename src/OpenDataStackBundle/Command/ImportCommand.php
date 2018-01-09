<?php

namespace OpenDataStackBundle\Command;

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
            $indexName = 'dkan-' . $udid . '-' . $resourceId;
            if ($client->indices()->exists(['index' => $indexName])) {
                $client->indices()->delete(['index' => $indexName]);
            }
            $response = $client->indices()->create(['index' => $indexName]);

            // 4. Batch index 1000 records at a time
            foreach ($records as $key => $rowFields) {
                $params['body'][] = [
                    'index' => [
                        '_index' => $indexName,
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

            // Get mappings for all types in 'my_index'.
            $params = [
                'field' => '*',
                'index' => $indexName,
                'include_defaults' => true
            ];

            // Update the kibana index-pattern.
            $response = $client->indices()->getFieldMapping($params);
            // Convert ES mapping to kibana mappings.
            $contract_mapping = $response[$indexName]['mappings']['contract'];
            $index_pattern_fields = array();
            foreach ($contract_mapping as $field_properties) {
                $field_full_name = $field_properties['full_name'];
                $field_mapping = array_pop($field_properties['mapping']);
                // Skip system fields that starts with '_'.
                if (strpos($field_full_name, '_') !== 0) {
                    $index_pattern_fields[] = array(
                        'name' => $field_full_name,
                        'type' => $field_mapping['type'],
                        'indexed' => $field_mapping['index'],
                        'doc_values' => $field_mapping['doc_values'],
                    );
                }
            }

            // Update Kibana index patterns fields mapping for all of the
            // .kibana indices.
            // Start by getting all of the available kibana own home indexs.
            $kibana_indices = $client->cat()->indices(array('index' => '.kibana*',));

            $kibana_indexpattern_id = 'dkan-' . $udid . '-*';

            $bulk_params = array('body' => array());
            foreach ($kibana_indices as $kibana_index) {
                $bulk_params['body'][] = array(
                    'update' => array(
                        '_index' => $kibana_index['index'],
                        '_type' => 'doc',
                        '_id' => 'index-pattern:' . $kibana_indexpattern_id,
                    )
                );

                $bulk_params['body'][] = array(
                    'doc_as_upsert' => 'true',
                    'doc' => array (
                        'type' => 'index-pattern',
                        'index-pattern' => array(
                            "title" => $kibana_indexpattern_id,
                            "fields" => json_encode($index_pattern_fields),
                        ),
                    ),
                );
            }

            $updateLogs = $client->bulk($bulk_params);
            var_dump(json_encode($index_pattern_fields));
            var_dump($updateLogs);

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
