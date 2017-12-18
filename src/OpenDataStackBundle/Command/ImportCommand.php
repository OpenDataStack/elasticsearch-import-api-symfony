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
        $connectionFactory = new FsConnectionFactory('/tmp/enqueue');
        $context = $connectionFactory->createContext();
        $importQueue = $context->createQueue('importQueue');
        $queueConsumer = new QueueConsumer($context);

        // Initialise Elasticsearch PHP Client
        $client = ClientBuilder::create()
            ->setHosts(['http://elasticsearch:9200'])
            ->setLogger(ClientBuilder::defaultLogger('/tmp/importer.log'))
            ->setSSLVerification(false)
            ->build();

        // anonymous block function to handle incoming requests
        $queueConsumer->bind('importQueue', function (PsrMessage $message) use (&$output, $client) {
            $output->writeln("Processing Job Import");

            // Parse payload from the REST call
            $data = json_decode($message->getBody(), true);
            $udid = $data['udid'];
            $resourceId = $data['id'];
            $output->writeln("Import Type: " . $data['importer']);
            $output->writeln("URI: " . $data['uri']);

            // 1. Download CSV Resource
            $uniqueFileName = vsprintf("resource_%s.csv", uniqid());
            $filePath = "/tmp/configurations/" . $data['udid'] . "/" . $uniqueFileName;
            $uri = $data['uri'];

            file_put_contents($filePath, fopen($uri, 'r'));
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
                        '_type' => 'contract'
                    ]
                ];
                $params['body'][] = $rowFields;

                // Every 1000 documents stop and send the bulk request
                if ($key % 1000 == 0) {
                    $response = $client->bulk($params);
                    print_r($response);

                    $params = ['body' => []];
                }
            }

            // Send the last batch
            if (!empty($params['body'])) {
                $response = $client->bulk($params);
                print_r($response);
            }

            return PsrProcessor::ACK;
        });

        $queueConsumer->consume();
    }
}
