<?php

namespace OpenDataStackBundle\Helper;

use Elasticsearch\Client;

/**
 *
 */
class KibanaHelper
{
    /**
     *
     */
    public static function kibanaGetFieldMapping(Client $client, $index = '') {
        $index_pattern_fields = array();
        // Get mappings for all types in 'my_index'.
        $params = [
            'field' => '*',
            'index' => $index,
            'include_defaults' => true
        ];

        $response = $client->indices()->getFieldMapping($params);

        // Convert ES mapping to kibana mappings.
        $contract_mapping = array();

        // Get all the mappitngs.
        foreach ($response as $index => $response_index) {
            // We are only interested in Dkan indices.
            if (strpos($index, 'dkan-') === 0) {
                foreach ($response_index['mappings'] as $mapping_name => $mapping_data) {
                    $contract_mapping = array_merge($contract_mapping, $mapping_data);
                }
            }
        }

        // Process the mappings.
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

        return $index_pattern_fields;
    }

    /**
     *
     */
    public static function kibanaUpsertIndexPattern(Client $client, $kibana_indexpattern_id, $kibana_indexpattern_title, $kibana_indexpattern_fields = array(), &$logs) {
        // Get all of the available kibana indexs.
        $kibana_indices = $client->cat()->indices(array('index' => '.kibana*',));
        if (!empty($kibana_indices)) {
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
                            "title" => $kibana_indexpattern_title,
                            "fields" => json_encode($kibana_indexpattern_fields),
                        ),
                    ),
                );
            }
            $updateLogs = $client->bulk($bulk_params);
        }
    }
}
