import pytest
import pandas as pd
import pvoutput


def test_date_to_pvoutput_str():
    VALID_DATE_STR = "20190101"
    assert pvoutput.date_to_pvoutput_str(VALID_DATE_STR) == VALID_DATE_STR
    assert pvoutput.date_to_pvoutput_str(pd.Timestamp(VALID_DATE_STR)) == VALID_DATE_STR
    
    
def test_check_date():
    assert pvoutput._check_date("20190101") is None
    with pytest.raises(ValueError):
        pvoutput._check_date("2010")
    with pytest.raises(ValueError):
        pvoutput._check_date("2010-01-02")
        
        
def test_check_pv_system_status():
    def _make_timeseries(start, end):        
        index = pd.date_range(start, end, freq="5T")
        timeseries = pd.DataFrame(None, index=index)
        return timeseries
        
    good_timeseries = _make_timeseries("2019-01-01 00:00", "2019-01-02 00:00")
    pvoutput.check_pv_system_status(good_timeseries, "20190101")
    
    bad_timeseries = _make_timeseries("2019-01-01 00:00", "2019-01-03 00:00")
    with pytest.raises(ValueError):
        pvoutput.check_pv_system_status(bad_timeseries, "20190101")
        
    bad_timeseries2 = _make_timeseries("2019-01-02 00:00", "2019-01-03 00:00")
    with pytest.raises(ValueError):
        pvoutput.check_pv_system_status(bad_timeseries2, "20190101")