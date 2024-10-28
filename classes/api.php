<?php
// This file is part of Moodle - http://moodle.org/
//
// Moodle is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Moodle is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with Moodle.  If not, see <http://www.gnu.org/licenses/>.

namespace local_azureblobstorage;

use GuzzleHttp\Client;
use GuzzleHttp\Promise\Promise;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Promise\Utils;
use GuzzleHttp\Psr7\Request;
use Psr\Http\Message\StreamInterface;
use coding_exception;

/**
 * Azure blob storage API.
 *
 * This class is intended to generically implement basic blob storage operations (get,put,delete,etc...)
 * which can then be referenced in other plugins.
 *
 * @package    local_azureblobstorage
 * @author     Matthew Hilton <matthewhilton@catalyst-au.net>
 * @copyright  2024 Catalyst IT
 * @license    http://www.gnu.org/copyleft/gpl.html GNU GPL v3 or later
 */
class api {
    /**
     * @var Client Guzzle HTTP client for making requests
     */
    private readonly Client $client;

    /**
     * @var int Threshold before blob uploads using multipart upload.
     */
    const MULTIPART_THRESHOLD = 32 * 1024 * 1024; // 32MB.

    /**
     * @var int Number of bytes per multipart block.
     *
     * As of 2019-12-12 api version the max size is 4000MB.
     * @see https://learn.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs#about-block-blobs
     */
    const MULTIPART_BLOCK_SIZE = 32 * 1024 * 1024; // 32MB.

    /**
     * @var int Maximum number of blocks allowed. This is set by Azure.
     * @see https://learn.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs#about-block-blobs
     */
    const MAX_NUMBER_BLOCKS = 50000;

    /**
     * @var int Maximum block size. This is set by azure
     * @see https://learn.microsoft.com/en-us/azure/storage/blobs/scalability-targets
     */
    const MAX_BLOCK_SIZE = 50000 * 4000 * 1024; // 50,000 x 4000 MB blocks, approx 190 TB

    /**
     * @var string the default content type if none is given.
     */
    const DEFAULT_CONTENT_TYPE = 'application/octet-stream';

    /**
     * Create a API
     * @param string $account Azure storage account name
     * @param string $container Azure storage container name (inside the given storage account).
     * @param string $sastoken SAS (Shared access secret) token for authentication.
     */
    public function __construct(
        /** @var string Azure storage account name */
        public readonly string $account,
        /** @var string Azure storage container name */
        public readonly string $container,
        /** @var string SAS token for authentication */
        public readonly string $sastoken
    ) {
        $this->client = new Client();
    }

    /**
     * URL for blob
     * @param string $blobkey key of blob
     * @return string
     */
    private function build_blob_url(string $blobkey): string {
        return 'https://' . $this->account . '.blob.core.windows.net/' . $this->container . '/' . $blobkey . '?' . $this->sastoken;
    }

    /**
     * Blob block URL. Blocks are 'pieces' of a blob.
     * @param string $blobkey key of blob
     * @param string $blockid id of block. Note, for each blob, every blockid must have the exact same length and is base64 encoded.
     * @see https://learn.microsoft.com/en-us/rest/api/storageservices/put-block
     * @return string
     */
    private function build_blob_block_url(string $blobkey, string $blockid): string {
        return $this->build_blob_url($blobkey) . '&comp=block&blockid=' . $blockid;
    }

    /**
     * Builds block list url. Block list of a list of blocks.
     * @param string $blobkey key of blob
     * @return string
     */
    private function build_blocklist_url(string $blobkey): string {
        return $this->build_blob_url($blobkey) . '&comp=blocklist';
    }

    /**
     * Build blob properties URL.
     * @param string $blobkey key of blob
     * @return string
     */
    private function build_blob_properties_url(string $blobkey): string {
        return $this->build_blob_url($blobkey) . '&comp=properties';
    }

    /**
     * Get blob.
     * @param string $key blob key
     * @return PromiseInterface Promise that resolves a ResponseInterface value where the body is a stream of the blob contents.
     */
    public function get_blob_async(string $key): PromiseInterface {
        // Enable streaming response, useful for large files e.g. videos.
        return $this->client->getAsync($this->build_blob_url($key), ['stream' => true]);
    }

    /**
     * Get blob properties.
     * @param string $key blob key
     * @return PromiseInterface Promise that resolves a ResponseInterface value where the properties are in the response headers.
     */
    public function get_blob_properties_async(string $key): PromiseInterface {
        return $this->client->headAsync($this->build_blob_url($key));
    }

    /**
     * Deletes a given blob
     * @param string $key blob key
     * @return PromiseInterface Promise that resolves once the delete request succeeds.
     */
    public function delete_blob_async(string $key): PromiseInterface {
        return $this->client->deleteAsync($this->build_blob_url($key));
    }

    /**
     * Put (create/update) blob.
     * Note depending on the size of the stream, it may be uploaded via single or multipart upload.
     *
     * @param string $key blob key
     * @param StreamInterface $contentstream the blob contents as a stream
     * @param string $md5 binary md5 hash of file contents. You likely need to call hex2bin before passing in here.
     * @param string $contenttype Content type to set for the file.
     * @return PromiseInterface Promise that resolves a ResponseInterface value.
     */
    public function put_blob_async(string $key, StreamInterface $contentstream, string $md5,
        string $contenttype = self::DEFAULT_CONTENT_TYPE): PromiseInterface {
        if ($this->should_stream_upload_multipart($contentstream)) {
            return $this->put_blob_multipart_async($key, $contentstream, $md5, $contenttype);
        } else {
            return $this->put_blob_single_async($key, $contentstream, $md5, $contenttype);
        }
    }

    /**
     * Puts a blob using single upload. Suitable for small blobs.
     *
     * @param string $key blob key
     * @param StreamInterface $contentstream the blob contents as a stream
     * @param string $md5 binary md5 hash of file contents. You likely need to call hex2bin before passing in here.
     * @param string $contenttype Content type to set for the file.
     * @return PromiseInterface Promise that resolves a ResponseInterface value.
     */
    public function put_blob_single_async(string $key, StreamInterface $contentstream, string $md5,
        string $contenttype = self::DEFAULT_CONTENT_TYPE): PromiseInterface {
        return $this->client->putAsync(
            $this->build_blob_url($key),
            [
                'headers' => [
                    'x-ms-blob-type' => 'BlockBlob',
                    'x-ms-blob-content-type' => $contenttype,
                    'content-md5' => base64_encode($md5),
                ],
                'body' => $contentstream,
            ]
        );
    }

    /**
     * Puts a blob using multipart/block upload. Suitable for large blobs.
     * This is done by splitting the blob into multiple blocks, and then combining them using a BlockList on the Azure side
     * before finally setting the final md5 by setting the blob properties.
     *
     * @param string $key blob key
     * @param StreamInterface $contentstream the blob contents as a stream
     * @param string $md5 binary md5 hash of file contents. You likely need to call hex2bin before passing in here.
     * @param string $contenttype Content type to set for the file.
     * @return PromiseInterface Promise that resolves when complete. Note the response is NOT available here,
     * because this operation involves many separate requests.
     */
    public function put_blob_multipart_async(string $key, StreamInterface $contentstream, string $md5,
        string $contenttype = self::DEFAULT_CONTENT_TYPE): PromiseInterface {
        // We make multiple calls to the Azure API to do multipart uploads, so wrap the entire thing
        // into a single promise.
        $entirepromise = new Promise(function() use (&$entirepromise, $key, $contentstream, $md5, $contenttype) {
            // Split into blocks.
            $counter = 0;
            $blockids = [];
            $promises = [];

            while (true) {
                $content = $contentstream->read(self::MULTIPART_BLOCK_SIZE);

                // Each block has its own md5 specific to itself.
                $blockmd5 = base64_encode(hex2bin(md5($content)));

                // Finished reading, nothing more to upload.
                if (empty($content)) {
                    break;
                }

                // The block ID must be the same length regardles of the counter value.
                // So pad them with zeros.
                $blockid = base64_encode(
                    str_pad($counter++, 6, '0', STR_PAD_LEFT)
                );

                $request = new Request('PUT', $this->build_blob_block_url($key, $blockid), ['content-md5' => $blockmd5], $content);
                $promises[] = $this->client->sendAsync($request);
                $blockids[] = $blockid;
            };

            if (count($blockids) > self::MAX_NUMBER_BLOCKS) {
                throw new coding_exception("Max number of blocks reached, block size too small ?");
            }

            // Will throw exception if any fail - if any fail we want to abort early.
            Utils::unwrap($promises);

            // Commit the blocks together into a single blob.
            $body = $this->make_block_list_xml($blockids);
            $bodymd5 = base64_encode(hex2bin(md5($body)));
            $request = new Request('PUT', $this->build_blocklist_url($key),
                ['Content-Type' => 'application/xml', 'content-md5' => $bodymd5], $body);
            $this->client->send($request);

            // Now it is combined, set the md5 and content type on the completed blob.
            $request = new Request('PUT', $this->build_blob_properties_url($key), [
                'x-ms-blob-content-md5' => base64_encode($md5),
                'x-ms-blob-content-type' => $contenttype,
            ]);
            $this->client->send($request);

            // Done, resolve the entire promise.
            $entirepromise->resolve('fulfilled');
        });

        return $entirepromise;
    }

    /**
     * If the stream should upload using multipart upload.
     * @param StreamInterface $stream
     * @return bool
     */
    private function should_stream_upload_multipart(StreamInterface $stream): bool {
        return $stream->getSize() > self::MULTIPART_THRESHOLD;
    }

    /**
     * Generates a blocklist XML.
     * @see https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list#request-body
     * @param array $blockidlist list of block ids.
     * @return string blocklist xml string.
     */
    private function make_block_list_xml(array $blockidlist): string {
        // We use 'Latest' since we don't care about committing different
        // blob block versions - we always want the latest.
        $string = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<BlockList>";
        foreach ($blockidlist as $blockid) {
            $string .= "\n<Latest>" . $blockid . '</Latest>';
        }
        $string .= "\n</BlockList>";
        return $string;
    }
}
