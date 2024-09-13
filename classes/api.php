<?php

namespace local_azureblobstorage;

use core\exception\coding_exception;
use GuzzleHttp\Client;
use GuzzleHttp\Promise\Promise;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Promise\Utils;
use GuzzleHttp\Psr7\Request;
use Psr\Http\Message\StreamInterface;

class api {
    /**
     * @var string Storage account name
     */
    private string $account;

    /**
     * @var string Storage account container name
     */
    private string $container;

    /**
     * @var Shared Access Token (SAS) for authentication
     */
    private string $sastoken;

    /**
     * @var Client Guzzle HTTP client for making requests
     */
    private Client $client;

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

    public function __construct(string $account, string $container, string $sastoken) {
        $this->account = $account;
        $this->container = $container;
        $this->sastoken = $sastoken;
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
     * @param string $blockid id of block. TODO note format or docs link
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
    public function get_blob(string $key): PromiseInterface {
        return $this->client->getAsync($this->build_blob_url($key));
    }

    /**
     * Get blob properties.
     * @param string $key blob key
     * @return PromiseInterface Promise that resolves a ResponseInterface value where the properties are in the response headers.
     */
    public function get_blob_properties(string $key): PromiseInterface {
        return $this->client->headAsync($this->build_blob_url($key));
    }

    /**
     * Put (create/update) blob.
     * Note depending on the size of the stream, it may be uploaded via single or multipart upload.
     * 
     * @param string $key blob key
     * @param StreamInterface $contentstream the blob contents as a stream
     * @param string $md5 binary md5 hash of file contents. You likely need to call hex2bin before passing in here.
     * @return PromiseInterface Promise that resolves a ResponseInterface value.
     */
    public function put_blob(string $key, StreamInterface $contentstream, string $md5): PromiseInterface {
        if ($this->should_stream_upload_multipart($contentstream)) {
            return $this->put_blob_multipart($key, $contentstream, $md5);
        } else {
            return $this->put_blob_single($key, $contentstream, $md5);
        }
    }

    /**
     * Puts a blob using single upload. Suitable for small blobs.
     *
     * @param string $key blob key
     * @param StreamInterface $contentstream the blob contents as a stream
     * @param string $md5 binary md5 hash of file contents. You likely need to call hex2bin before passing in here.
     * @return PromiseInterface Promise that resolves a ResponseInterface value.
     */
    public function put_blob_single(string $key, StreamInterface $contentstream, string $md5): PromiseInterface {
        return $this->client->putAsync(
            $this->build_blob_url($key),
            [
                'headers' => [
                    'x-ms-blob-type' => 'BlockBlob',
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
     * @return PromiseInterface Promise that resolves when complete.
     */
    public function put_blob_multipart(string $key, StreamInterface $contentstream, string $md5): PromiseInterface {
        // We make multiple calls to the Azure API to do multipart uploads, so wrap the entire thing
        // into a single promise.
        $entirepromise = new Promise(function() use (&$entirepromise, $key, $contentstream, $md5) {
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
                // TODO different ex, handle better
                throw new coding_exception("Max number of blocks reached, block size too small ?");
            }

            // Will throw exception if any fail - if any fail we want to abort early.
            Utils::unwrap($promises);

            // Commit the blocks together into a single blob.
            $body = $this->make_block_list_xml($blockids);
            $bodymd5 = base64_encode(hex2bin(md5($body)));
            $request = new Request('PUT', $this->build_blocklist_url($key), ['Content-Type' => 'application/xml', 'content-md5' => $bodymd5], $body);
            $this->client->send($request);

            // Now it is combined, set the md5 on the completed blob.
            $request = new Request('PUT', $this->build_blob_properties_url($key), ['x-ms-blob-content-md5' => base64_encode($md5)]);
            $this->client->send($request);

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
        foreach($blockidlist as $blockid) {
            $string .= "\n<Latest>" . $blockid . '</Latest>';
        }
        $string .= "\n</BlockList>";
        return $string;
    }
}