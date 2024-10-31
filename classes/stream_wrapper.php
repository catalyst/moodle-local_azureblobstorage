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

use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Psr7\CachingStream;
use GuzzleHttp\Psr7\Utils;
use local_azureblobstorage\api;
use HashContext;
use Psr\Http\Message\StreamInterface;

/**
 * Azure Blob Storage stream wrapper to use "blob://<container>/<key>" files with PHP.
 *
 * Implementation references,
 * https://github.com/aws/aws-sdk-php/blob/master/src/S3/StreamWrapper.php
 * https://phpazure.codeplex.com/SourceControl/latest#trunk/library/Microsoft/WindowsAzure/Storage/Blob/Stream.php
 *
 * @package    local_azureblobstorage
 * @author     Matthew Hilton <matthewhilton@catalyst-au.net>
 * @copyright  Catalyst IT
 * @license    http://www.gnu.org/copyleft/gpl.html GNU GPL v3 or later
 */
class stream_wrapper {

    /** @var resource|null Stream context (this is set by PHP) */
    public $context;

    /** @var StreamInterface Underlying stream resource */
    private $body;

    /** @var int Size of the body that is opened */
    private $size;

    /** @var string Mode in which the stream was opened */
    private $mode;

    /** @var string The opened protocol (e.g. "blob") */
    private $protocol = 'blob';

    /** @var HashContext Hash resource that is sent when flushing the file to Azure. */
    private $hash;

    /** @var bool records whether the file was readable when validating the stream_handle */
    private $readable = true;

    /** @var string The key extracted from the path from when the stream was opened */
    private $key = null;

    /**
     * Register the blob://' stream wrapper
     *
     * @param api $client Client to use with the stream wrapper
     * @param string $protocol Protocol to register as.
     */
    public static function register(api $client, $protocol = 'blob') {
        if (in_array($protocol, stream_get_wrappers())) {
            stream_wrapper_unregister($protocol);
        }

        stream_wrapper_register($protocol, get_called_class(), STREAM_IS_URL);
        $default = stream_context_get_options(stream_context_get_default());
        $default[$protocol]['client'] = $client;
        stream_context_set_default($default);
    }

    /**
     * Does not support casting, always returns false.
     * @param mixed $castas
     * @return bool
     */
    public function stream_cast($castas): bool {
        return false;
    }

    /**
     * Closes the stream
     */
    public function stream_close() {
        $this->body = null;
        $this->hash = null;
    }

    /**
     * Opens the stream, depending on the mode.
     * @param mixed $path filepath
     * @param mixed $mode file mode constants, see fopen
     * @param mixed $options Additional flags
     * @param mixed $openedpath
     * @return bool True if successful, else false
     * @see https://www.php.net/manual/en/function.fopen.php
     */
    public function stream_open($path, $mode, $options, &$openedpath): bool {
        // Set the protocol.
        $this->initProtocol($path);
        $this->key = $this->get_key_from_path($path);

        // Trim 'b' and 't' off end.
        // these are line ending flags used to handle Unix/Windows line endings.
        // We don't care about these, so we just remove them.
        $this->mode = rtrim($mode, 'bt');

        // Check mode is valid for the given path.
        if ($errors = $this->validate($path, $this->mode)) {
            return $this->triggerError($errors);
        }

        $this->hash = hash_init('md5');

        // Call stream based on the mode.
        return $this->boolCall(function() use ($path) {
            switch ($this->mode) {
                case 'r':
                    return $this->open_read_stream();
                case 'a':
                    return $this->open_append_stream();
                default:
                    return $this->open_write_stream();
            }
        });
    }

    /**
     * Has stream reached end of file ?
     * @return bool
     */
    public function stream_eof(): bool {
        return $this->body->eof();
    }

    /**
     * Flushes (closes) the stream. This is where files are uploaded to Azure.
     * @return bool
     */
    public function stream_flush() {
        // Cannot write in readonly mode, exit.
        if ($this->mode == 'r') {
            return false;
        }

        // Return to start of stream.
        if ($this->body->isSeekable()) {
            $this->body->seek(0);
        }

        // Get the hash of the file, used as a checksum in Azure.
        // Azure will reject it on the server side if the given MD5
        // does not match what they receive.
        $md5 = hex2bin(hash_final($this->hash));

        // Upload the blob. Under the hood this may be a multipart upload if the file is large enough.
        $this->get_client()->put_blob_async($this->key, $this->body, $md5)->wait();
        return true;
    }

    /**
     * Reads the stream
     * @param int $count Number of bytes to read
     * @return string data returned from stream.
     */
    public function stream_read($count) {
        // If the file isn't readable, we need to return no content. Azure can emit XML here otherwise.
        return $this->readable ? $this->body->read($count) : '';
    }

    /**
     * Go to a position in the stream
     * @param int $offset
     * @param int $whence
     * @return bool if successful.
     */
    public function stream_seek($offset, $whence = SEEK_SET) {
        // Cannot seek if underlying body is not seekable.
        if (!$this->body->isSeekable()) {
            return false;
        }

        return $this->boolCall(function () use ($offset, $whence) {
            $this->body->seek($offset, $whence);
            return true;
        });
    }

    /**
     * Returns current position of stream.
     * @return int
     */
    public function stream_tell() {
        return $this->body->tell();
    }

    /**
     * Write to the stream.
     * @param string $data
     * @return int Number of bytes successfully written.
     */
    public function stream_write($data) {
        // Update the md5 hash as we go along,
        // it is used for verification when uploading to Azure.
        hash_update($this->hash, $data);
        return $this->body->write($data);
    }

    /**
     * Get stats about the stream
     * @return array
     */
    public function stream_stat() {
        $stat = $this->getStatTemplate();
        $stat[7] = $stat['size'] = $this->get_size();
        $stat[2] = $stat['mode'] = $this->mode;

        return $stat;
    }

    /**
     * url_stat
     *
     * Provides information for is_dir, is_file, filesize, etc. Works on
     * buckets, keys, and prefixes.
     * @link http://www.php.net/manual/en/streamwrapper.url-stat.php
     *
     * @param string $path
     * @param mixed $flags
     *
     * @return mixed
     */
    public function url_stat($path, $flags) {
        $stat = $this->getStatTemplate();

        try {
            $key = $this->get_key_from_path($path);
            $res = $this->get_client()->get_blob_properties_async($key)->wait();

            $contentlength = current($res->getHeader('Content-Length'));
            $lastmodified = strtotime(current($res->getHeader('Last-Modified')));

            $stat['size'] = $stat[7] = $contentlength;
            $stat['mtime'] = $stat[9] = $lastmodified;
            $stat['ctime'] = $stat[10] = $lastmodified;

            // Regular file with 0777 access - see "man 2 stat".
            $stat['mode'] = $stat[2] = 0100777;

            return $stat;

            // ClientException is thrown on 4xx errors e.g. 404.
        } catch (ClientException $ex) {
            // The specified blob does not exist.
            return false;
        }
    }

    /**
     * Unlinks (deletes) a given file.
     * @param string $path
     * @return bool if successful
     */
    public function unlink(string $path): bool {
        return $this->boolcall(function() use ($path) {
            $client = $this->get_client();
            $key = $this->get_key_from_path($path);
            $client->delete_blob_async($key)->wait();
            return true;
        });
    }

    /**
     * Parse the protocol out of the given path.
     *
     * @param string $path
     */
    private function initprotocol($path) {
        $parts = explode('://', $path, 2);
        $this->protocol = $parts[0] ?: 'blob';
    }

    /**
     * Extracts the blob key from the given filepath (filepath is usually blob://key)
     * @param string $path
     * @return string|null
     */
    private function get_key_from_path(string $path): ?string {
        // Remove the protocol.
        $parts = explode('://', $path);
        return $parts[1] ?: null;
    }

    /**
     * Validates the provided stream arguments for fopen
     * @param string $path
     * @param string $mode
     * @return array of error messages, or empty if ok.
     */
    private function validate($path, $mode): array {
        $errors = [];

        // Ensure the key is correctly set in the options.
        // it might not have been parsed correctly.
        if (!$this->key) {
            $errors[] = 'Could not parse the filepath. You must specify a path in the '
                . 'form of blob://container/key';
        }

        // Ensure mode is valid, we don't support every mode.
        if (!in_array($mode, ['r', 'w', 'a', 'x'])) {
            $errors[] = "Mode not supported: {$mode}. "
                . "Use one 'r', 'w', 'a', or 'x'.";
        }

        $key = $this->get_key_from_path($path);
        $blobexists = $this->blob_exists($key);

        // When using mode "x" validate if the file exists before attempting to read.
        if ($mode == 'x' && $blobexists) {
            $errors[] = "{$path} already exists on Azure Blob Storage";
        }

        // When using mode 'r' we should validate the file exists before opening a handle on it.
        if ($mode == 'r' && !$blobexists) {
            $errors[] = "{$path} does not exist on Azure Blob Storage";
            $this->readable = false;
        }

        return $errors;
    }

    /**
     * Determines if a blob exists in azure.
     * @param string $key
     * @return bool true if exists, else false.
     */
    private function blob_exists(string $key): bool {
        try {
            $this->get_client()->get_blob_properties_async($key)->wait();

            // No exception, blob exists.
            return true;
        } catch (ClientException $e) {
            // Exception was 404 indicating it connected, but the blob did not exist.
            if ($e->getResponse()->getStatusCode() == 404) {
                return false;
            }

            // Else another error ocurred, re-throw.
            throw $e;
        }
    }

    /**
     * Get the stream context options available to the current stream
     * @return array
     */
    private function get_options(): array {
        // Context is not set when doing things like stat.
        if ($this->context === null) {
            $options = [];
        } else {
            $options = stream_context_get_options($this->context);
            $options = isset($options[$this->protocol])
                ? $options[$this->protocol]
                : [];
        }

        $default = stream_context_get_options(stream_context_get_default());
        $default = isset($default[$this->protocol])
            ? $default[$this->protocol]
            : [];
        $result = $options + $default;

        return $result;
    }

    /**
     * Get a specific stream context option
     *
     * @param string $name Name of the option to retrieve
     *
     * @return mixed|null
     */
    private function get_option($name) {
        $options = $this->get_options();
        return isset($options[$name]) ? $options[$name] : null;
    }

    /**
     * Gets the client.
     *
     * @return api
     * @throws \RuntimeException if no client has been configured
     */
    private function get_client() {
        if (!$client = $this->get_option('client')) {
            throw new \RuntimeException('No client in stream context');
        }

        return $client;
    }

    /**
     * Opens a readable stream.
     * @return bool True if successful, else false.
     */
    private function open_read_stream() {
        $client = $this->get_client();

        try {
            $res = $client->get_blob_async($this->key)->wait();
            $this->body = $res->getBody();
        } catch (ClientException $e) {
            // Could not open stream.
            return false;
        }

        // Wrap the body in a caching entity body if seeking is allowed.
        if ($this->get_option('seekable') && !$this->body->isSeekable()) {
            $this->body = new CachingStream($this->body);
        }

        return true;
    }

    /**
     * Opens a stream for writing.
     * @return bool True if successfull.
     */
    private function open_write_stream() {
        // A writeable stream is actually just a stream to a temp file.
        // the actual Azure upload only takes place once the stream is flushed (i.e. closed).
        $this->body = Utils::streamFor(fopen('php://temp', 'r+'));
        return true;
    }

    /**
     * Opens a stream to append to a file.
     * @return bool
     */
    private function open_append_stream(): bool {
        try {
            // Get the body of the object and seek to the end of the stream.
            $client = $this->get_client();
            $this->body = $client->get_blob_async($this->key)->wait()->getBody();
            $this->body->seek(0, SEEK_END);
            return true;

            // Client exceptions are thrown on 4xx errors, e.g. 404.
        } catch (ClientException $e) {
            // The object does not exist, so use a simple write stream.
            return $this->open_write_stream();
        }
    }

    /**
     * Gets a URL stat template with default values
     * These are returned in both numeric and associative values
     * @see https://www.php.net/manual/en/function.stat.php
     * @return array
     */
    private function getstattemplate() {
        return [
            0  => 0,  'dev'     => 0,
            1  => 0,  'ino'     => 0,
            2  => 0,  'mode'    => 0,
            3  => 0,  'nlink'   => 0,
            4  => 0,  'uid'     => 0,
            5  => 0,  'gid'     => 0,
            6  => -1, 'rdev'    => -1,
            7  => 0,  'size'    => 0,
            8  => 0,  'atime'   => 0,
            9  => 0,  'mtime'   => 0,
            10 => 0,  'ctime'   => 0,
            11 => -1, 'blksize' => -1,
            12 => -1, 'blocks'  => -1,
        ];
    }

    /**
     * Invokes a callable and triggers an error if an exception occurs while
     * calling the function.
     * @param callable $fn
     * @param int $flags
     * @return bool
     */
    private function boolcall(callable $fn, $flags = null): bool {
        try {
            return $fn();
        } catch (\Exception $e) {
            return $this->triggerError($e->getMessage(), $flags);
        }
    }

    /**
     * Trigger one or more errors
     *
     * @param string|array $errors Errors to trigger
     * @param mixed        $flags  If set to STREAM_URL_STAT_QUIET, then no
     *                             error or exception occurs
     * @return bool Returns false
     * @throws \RuntimeException if throw_errors is true
     */
    private function triggererror($errors, $flags = null): bool {
        // This is triggered with things like file_exists().
        if ($flags & STREAM_URL_STAT_QUIET) {
            return $flags & STREAM_URL_STAT_LINK
                // This is triggered for things like is_link().
                ? $this->getStatTemplate()
                : false;
        }

        // Redact the SAS token from the error to avoid accidental leakage.
        $errormsg = implode("\n", (array) $errors);
        $sastoken = $this->get_client()->sastoken;
        $errormsg = str_replace($sastoken, 'SAS_TOKEN_REDACTED', $errormsg);

        // This is triggered when doing things like lstat() or stat().
        trigger_error($errormsg, E_USER_WARNING);
        return false;
    }

    /**
     * Returns the size of the opened object body.
     * @return int|null
     */
    private function get_size(): ?int {
        $size = $this->body->getSize();
        return $size !== null ? $size : $this->size;
    }
}
