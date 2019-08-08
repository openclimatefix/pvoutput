Download historical solar photovoltaic data from [PVOutput.org](https://pvoutput.org).


This code is a work-in-progress.  The aim is to provide both a Python library for interacting with [PVOutput.org's API](https://pvoutput.org/help.html#api), and a set of scripts for downloading lots of data :)

# Installation

## Register with PVOutput.org

As well as an API key, you *need* a SystemId to use PVOutput's API.
If you don't have a PV system, click the "energy consumption only" box
when setting a system id on PVOutput.  If you don't include a
SystemId, then you'll get a "401 Unauthorized" response from the PVOutput API.

Set the environment variables `PVOUTPUT_APIKEY` and `PVOUTPUT_SYSTEMID`
(on Linux, put `EXPORT PVOUTPUT_APIKEY="API KEY"` etc. into `~/.profile`,
log out, and log back in again)

## Install pvoutput Python library

TODO

# Usage

TODO
