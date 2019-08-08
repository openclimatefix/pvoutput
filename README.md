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

`pip install -e git+https://github.com/openclimatefix/pvoutput_downloader#egg=pvoutput`


# Usage

```python
In [1]: import pvoutput

# Search for PV systems within 5km of a latitude and longitude
In [2]: pv_systems = pvoutput.pv_system_search('5km', lat=52.0668589, lon=-1.3484038)

In [3]: pv_systems.head()
Out[3]:
                     system_name  system_size_watts             postcode  ... distance_km  latitude longitude
system_id                                                                 ...
68309                Thorn House               3960  United Kingdom OX16  ...         1.0     52.06     -1.34
61190               Banbury OX16               5600  United Kingdom OX16  ...         1.0     52.06     -1.34
7191       Banbury Power Plant 2               3920  United Kingdom OX16  ...         1.0     52.06     -1.34
62751                Banbury PW2               5600  United Kingdom OX16  ...         1.0     52.06     -1.34
38924            CotonMacy Solar               3900  United Kingdom OX16  ...         1.0     52.06     -1.34

[5 rows x 11 columns]

In [4]: pvoutput.get_pv_metadata(pv_system_id=68309)
Out[4]: 
system_name                                      Thorn House
system_size_watts                                       3960
postcode                                                OX16
number_of_panels                                          12
panel_power_watts                                        330
panel_brand                     Evolution Ultra PLM-330MB-66
num_inverters                                              1
inverter_power_watts                                    3600
inverter_brand                                       Growatt
orientation                                               SE
array_tilt_degrees                                         1
shade                                                     No
install_date                                             NaT
latitude                                               52.06
longitude                                              -1.34
status_interval_minutes                                    5
number_of_panels_secondary                                 0
panel_power_watts_secondary                                0
orientation_secondary                                    NaN
array_tilt_degrees_secondary                             NaN
system_id                                              68309
Name: 68309, dtype: object
```
