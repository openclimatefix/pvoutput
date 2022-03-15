<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-7-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->

[![codecov](https://codecov.io/gh/openclimatefix/pvoutput/branch/main/graph/badge.svg?token=GTQDR2ZZ2S)](https://codecov.io/gh/openclimatefix/pvoutput)

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

The default location of the `.pvoutput.yml` is the user's home directory, expanded from `~`. This can be overridden by setting the `PVOUTPUT_CONFIG` environment variable.

e.g. `export PVOUTPUT_CONFIG="/my/preferred/location/.pvoutput.yml"`

Alternatively, you can set `API_KEY`, `SYSTEM_ID` and `DATA_SERVICE_URL` (see below) as environmental variables.

### API quotas and paid subscriptions
Please see [here](https://pvoutput.org/help/data_services.html) for update info.

#### Free

PVOutput.org gives you 60 API requests per hour.  Per request, you can download one day of data for one PV system.  (See PVOutput's docs for more info about [rate limits](https://pvoutput.org/help/api_specification.html#rate-limits).)

#### Donate
[Donating to PVOutput.org](https://pvoutput.org/help/donations.html#donations) increases your quota for a year to 300 requests per hour.

#### Paid
To get more historical data, you can pay $600 Australian dollars for a year's 'Live System History' subscription for a single country ([more info here](https://pvoutput.org/help/data_services.html). And [here's PVOutput.org's full price list](https://pvoutput.org/services.jsp)).
This allows you to use the [`get batch status`](https://pvoutput.org/help/data_services.html#get-batch-status-service) API to download 900 PV-system-*years* per hour.

If you have subscribed to PVOutput's data service then either
- add `data_service_url` to your configuration file (`~/.pvoutput.yml`) or
- pass `data_service_url` to the `PVOutput` constructor.

The `data_service_url` should end in `.org` (note this dones include the `/service/r2` part of the URL)
For example: `data_service_url: https://pvoutput.org/`


## Install pvoutput Python library

`pip install -e git+https://github.com/openclimatefix/pvoutput#egg=pvoutput`


# Usage

See the [Quick Start notebook](examples/quick_start.ipynb).

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tr>
    <td align="center"><a href="http://jack-kelly.com"><img src="https://avatars.githubusercontent.com/u/460756?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Jack Kelly</b></sub></a><br /><a href="https://github.com/openclimatefix/pvoutput/commits?author=JackKelly" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/ssmssam"><img src="https://avatars.githubusercontent.com/u/39378848?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Sam Murphy-Sugrue</b></sub></a><br /><a href="https://github.com/openclimatefix/pvoutput/commits?author=ssmssam" title="Code">💻</a></td>
    <td align="center"><a href="https://gabrieltseng.github.io/"><img src="https://avatars.githubusercontent.com/u/29063740?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Gabriel Tseng</b></sub></a><br /><a href="https://github.com/openclimatefix/pvoutput/commits?author=gabrieltseng" title="Code">💻</a></td>
    <td align="center"><a href="http://www.solar.sheffield.ac.uk/"><img src="https://avatars.githubusercontent.com/u/12187350?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Jamie Taylor</b></sub></a><br /><a href="https://github.com/openclimatefix/pvoutput/commits?author=JamieTaylor-TUOS" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/peterdudfield"><img src="https://avatars.githubusercontent.com/u/34686298?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Peter Dudfield</b></sub></a><br /><a href="#infra-peterdudfield" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a></td>
    <td align="center"><a href="https://github.com/vnshanmukh"><img src="https://avatars.githubusercontent.com/u/67438038?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Shanmukh Chava</b></sub></a><br /><a href="https://github.com/openclimatefix/pvoutput/commits?author=vnshanmukh" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/Antsthebul"><img src="https://avatars.githubusercontent.com/u/56587872?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Antsthebul</b></sub></a><br /><a href="https://github.com/openclimatefix/pvoutput/commits?author=Antsthebul" title="Code">💻</a></td>
  </tr>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!
