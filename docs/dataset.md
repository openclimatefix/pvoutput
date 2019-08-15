## `UK_PV_timeseries.hdf`

### `missing_dates` table

Sometimes we query PVOutput.api for a particular date and PV system ID, and PVOutput.org returns no data.  The `missing_dates` table records these pairs of PV system IDs and dates, so we don't retry these missing dates (and hence chew through our API quota!)

Columns:

- `pv_system_id`: index column, integer
- `missing_date_PV_localtime`: The date that we used to query to PVOutput.org API.  This is presumed to be in the timezone local to the PV system, although the PVOutput.org docs don't make this explicit.  `pd.HDFStore` doesn't support `date` columns, so these are actual `pd.Timestamp` objects.
- `datetime_of_API_request`: For data retrieved on or after 2019-08-06, this contains the UTC datetime of the API request.  For data retrieved between 2019-08-05 and 2019-08-06, this has been manually backfilled with '2019-08-05 00:00'.  For data retrieved before 2019-08-05, this columns contains `NaT` - these rows should be treated with some suspicion, because my data retrieval code may have been malformatting the date string for the PVOutput.org API, and hence may contain some 'missing dates' which aren't actually missing!  A tell-tale might be if there are duplicated rows.

### `metadata` table

### `timeseries/<pv_system_id>` tables

Columns:
- `datetime`: index column, pd.DatetimeIndex, [localtime to the PV system](https://forum.pvoutput.org/t/clarification-are-date-times-in-local-or-utc/570/2).
- `datetime_of_API_request_UTC`: The datetime at which we sent the API request.  Will be `NaT` for data retrieved before about 2019-08-06 13:00 UTC.
- `query_date`: The date (in localtime to the PV system) used in the query to the PVOutput.org API.  Will be `NaT` for data retrieved before about 2019-08-06 13:00 UTC.
- ... other columns contain data from PVOutput.org


