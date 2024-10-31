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

/**
 * Azure blob storage API
 *
 * @package    local_azureblobstorage
 * @author     Matthew Hilton <matthewhilton@catalyst-au.net>
 * @copyright  2024 Catalyst IT
 * @license    http://www.gnu.org/copyleft/gpl.html GNU GPL v3 or later
 */

defined('MOODLE_INTERNAL') || die();

$plugin->version   = 2024101400;      // The current plugin version (Date: YYYYMMDDXX).
$plugin->release   = 2024101400;      // Same as version.
$plugin->requires  = 2023042400;      // 4.2.0, PHP 8.0.0+
$plugin->component = "local_azureblobstorage";
$plugin->maturity  = MATURITY_ALPHA;
$plugin->supported = [402, 405];
