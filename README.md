[![Build Status](https://api.travis-ci.com/openclimatefix/pvoutput.svg)](https://travis-ci.com/openclimatefix/pvoutput/)

Download historical solar photovoltaic data from [PVOutput.org](https://pvoutput.org).

This code is a work-in-progress.  The aim is to provide both a Python library for interacting with [PVOutput.org's API](https://pvoutput.org/help.html#api), and a set of scripts for downloading lots of data :)

# Installation

## Register with PVOutput.org

You need to get an API key *and* a system ID from PVOutput.org.

If you don't have a PV system, click the "energy consumption only" box
when registering on PVOutput.  If you don't include a
system ID, then you'll get a "401 Unauthorized" response from the PVOutput API.

You can pass the API key and system ID into the `PVOutput` constructor.
Or, create a `~/.pvoutput.yml` file which looks like:

```yaml
api_key: <API key from PVOutput.org>
system_id: <SystemID from PVOutput.org>
```

If you have subscribed to PVOutput's commercial data service then add `data_service_url` to `~/.pvoutput.yml` or pass `data_service_url` to the `PVOutput` constructor.  The `data_service_url` should end in `.org`.  That is, don't include the `/service/r2` part of the URL.

## Install pvoutput Python library

`pip install -e git+https://github.com/openclimatefix/pvoutput#egg=pvoutput`


# Usage

See the [Quick Start notebook](examples/quick_start.ipynb).
