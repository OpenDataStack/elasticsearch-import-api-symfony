<?php

namespace OpenDataStackBundle\Helper;

use Symfony\Component\Filesystem\Filesystem;
use Elasticsearch\Client;

/**
 *
 */
class LogHelper
{
    /**
     * Persiste logs/config to the file system.
     *
     * @param Filesystem $fs
     * @param string  $inter_path
     * @param  $content
     * @param string  $filename
     * @param boolean  $update
     */
    public static function persisteJson($fs, $inter_path, $content, $filename, $update = TRUE)
    {
        $parent_dir = "/tmp/importer/configurations/";

        // Make sure the destination directory exists
        $destination_dir = "/tmp/importer/configurations/{$inter_path}/";

        if (!file_exists($destination_dir)) {
            $fs->mkdir($destination_dir, 0777, TRUE);
        }

        $destination_file = $destination_dir . $filename;

        $mode = 0;

        if ($fs->exists($destination_file)) {
            if ($update) {
                $mode = FILE_APPEND;
            }
            else {
                $fs->remove($destination_file);
            }
        }

        // Stringify the content.
        $data = json_encode($content);

        file_put_contents($destination_file, $data, $mode);
    }

    /**
     * Prepare logs message to be persisted later.
     *
     * @param string  $inter_path
     * @param array  $content
     * @param string  $filename
     * @param boolean  $update
     */
    public static function prepareLog($message, array $elasticsearch_return = array(), $created_at = null)
    {
        if (empty($created_at)) {
            $created_at = date('Y-m-d H:i:s');
        }

        return [
            "message" => $message,
            "created_at" => $created_at,
            "elasticsearch" => $elasticsearch_return,
        ];
    }
}
