<?php
/**
 * Created by PhpStorm.
 * User: noomane
 * Date: 14/12/17
 * Time: 15:28
 */

namespace OpenDataStackBundle\Command;

use Elasticsearch\ClientBuilder;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

use League\Csv\Reader;

class ElasticsearchCommand extends ContainerAwareCommand {

    protected function configure() {
        $this
            ->setName('elastic:debug')
            ->setDescription('debug elasticsearch php api');
    }

    protected function execute(InputInterface $input, OutputInterface $output) {

        // Init Elasticsearch client
        $client = ClientBuilder::create()
            ->setHosts(['http://elasticsearch:9200'])
            ->setLogger(ClientBuilder::defaultLogger('/tmp/csv-import.log'))
            ->setSSLVerification(FALSE)
            ->build();

        // Clear and recreate index based on import configuration
        // Elasticsearch index template
        $indexName = 'dkan-999-abc';
        if ($client->indices()->exists(['index' => $indexName])) {
            $client->indices()->delete(['index' => $indexName]);
        }
        $response = $client->indices()->create(['index' => $indexName]);

        // Download csv file to temp location
        $filePath = "/tmp/configurations/999/abc.csv";
        $uri = 'https://cdn.rawgit.com/achoura/elkd/aa0074ac/model_slug.csv';
        file_put_contents($filePath, fopen($uri, 'r'));

        // Intialize streaming low memory CSV reader
        $csv = Reader::createFromPath($filePath, 'r');
        $csv->setHeaderOffset(0);
        $header = $csv->getHeader();
        $records = $csv->getRecords();

        // Batch index 1000 records at a time
        foreach ($records as $key => $rowFields) {
            $params['body'][] = [
                'index' => [
                    '_index' => $indexName,
                    '_type' => 'row'
                ]
            ];
            $params['body'][] = $rowFields;

            // Every 1000 documents stop and send the bulk request
            if ($key % 10 == 0) {
                $responses = $client->bulk($params);
                print_r($responses);
                $params = ['body' => []];
            }
        }
        
    }
}