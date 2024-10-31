# Azure Blob Storage SDK - Moodle Plugin

A moodle plugin with functions to interact with the Microsoft Azure Blob Storage service.

## Why does this exist? ##

Since March 2024, Microsoft officially stopped supporting the Azure Storage for PHP SDK. This meant the code slowly became more and more out of date and is no longer receiving any updates.
This plugin intends to be a lightweight and simple implementation of only the Blob storage components (for example, it does not support queues, tables, etc...).

This is mainly used as a dependency when using Azure storage with tool_objectfs, see https://github.com/catalyst/moodle-tool_objectfs

## Supported Moodle Versions

| Branch           | Version support |  PHP Version |
| ---------------- | --------------- | ------------ |
| MOODLE_402_STABLE | 4.2 +           | 8.0.0+       |

## Installation

You can install this plugin from the plugin directory or get the latest version
on GitHub.

```bash
git clone https://github.com/catalyst/moodle-local_azureblobstorage local/azureblobstorage
```

## How to use
There are two usage options:
1. Call the API functions directly e.g. `api::put_blob`
2. Register the stream wrapper, which allows you to use PHP's built in file methods to move files e.g. `copy('/file.txt', 'blob://container/file')`

# Crafted by Catalyst IT


This plugin was developed by Catalyst IT Australia:

https://www.catalyst-au.net/

![Catalyst IT](/pix/catalyst-logo.png?raw=true)


# Contributing and Support

Issues, and pull requests using github are welcome and encouraged! 

https://github.com/catalyst/moodle-local_azureblobstorage/issues

If you would like commercial support or would like to sponsor additional improvements
to this plugin please contact us:

https://www.catalyst-au.net/contact-us
