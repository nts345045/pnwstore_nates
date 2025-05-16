"""
An abstraction of the WaveformClient class from
the `pnwstore` repository by Yiyu Ni
(https://github.com/niyiyu/pnwstore.git)

License: MIT License (c) 2022 Yiyu Ni

Modified by Nathan T. Stevens (2025)


"""
import sqlite3, io, obspy, os, warnings
import pandas as pd


mseedkeys = [
    "network",
    "station",
    "location",
    "channel",
    "quality",
    "version",
    "starttime",
    "endtime",
    "samplerate",
    "filename",
    "byteoffset",
    "bytes",
    "hash",
    "timeindex",
    "timespans",
    "timerates",
    "format",
    "filemodtime",
    "updated",
    "scanned",
]

def rst2df(result, keys=None):
    if isinstance(keys, str):
        return pd.DataFrame(result, columns=[keys])
    elif isinstance(keys, list):
        return pd.DataFrame(result, columns=keys)
    else:
        return pd.DataFrame(result)

def wildcard_mapper(c):
    if "*" in c:
        c = c.replace("*", "%")
    if "?" in c:
        c = c.replace("?", "_")
    return c

class WaveformClient(object):
    """
    Revised version of the WaveformClient class by Yiyu Ni
    that interfaces with a sqlite3 database populated by
    `mseedindex` (Chad Tremblant)

    :param sqlite: path and database name for the sqlite3 database to query
    :type sqlite: str
    """
    def __init__(self, sqlite):
        """Initialize a WaveformClient object

        :param sqlite: path and database name for the sqlite3 database to query
        :type sqlite: str
        """        
        self._db = sqlite3.connect(sqlite)
        self._cursor = self._db.cursor()
        self._keys = mseedkeys


    def get_waveforms(self, network, station, location, channel, starttime, endtime,
                      quality=None, minimumlength=None, longestonly=None, filename=None,
                      attach_response=False, **kwargs):
        """Query waveforms with the same syntax as the 
        :class:`~obspy.clients.fdsn.client.Client` calss
        :meth:`~.obspy.clients.fdsn.client.Client.get_waveforms`

        network, station, location, and channel codes can be:
         - explicit strings: network='UW'
         - wild-card containing strings: e.g., location='*', channel='?H?'
         - comma-delimited strings: e.g., station='MBW*,JCW'
        
        :param network: network code(s)
        :type network: str
        :param station: station code(s)
        :type station: str
        :param location: location code(s)
        :type location: str
        :param channel: channel code(s)
        :type channel: str
        :param starttime: starttime for NSLC(s) in this query
        :type starttime: obspy.core.utcdatetime.UTCDateTime
        :param endtime: endtime for NSLC(s) in this query
        :type endtime: obspy.core.utcdatetime.UTCDateTime
        :param quality: Specific SEED quality indicator, handling is
            source datacenter dependent, defaults to None
        :type quality: str, optional
        :param minimumlength: minimum trace length, defaults to None
            NOT IMPLEMENTED
        :type minimumlength: NoneType, optional
        :param longestonly: limit results to the longest continuous segment
            per channel, defaults to None
            NOT IMPLEMENTED
        :type longestonly: NoneType, optional
        :param filename: filename to save loaded stream to, defaults to None
        :type filename: str or NoneType, optional
        :param attach_response: should response information be attached
            to the retrieved stream? Defaults to False.
            If set to ```True```, a :class:`~obspy.Inventory` object or loadable
            filepath to a response-containing file readable by :meth:`~obspy.read_inventory`
            must be provided as an optional `inventory` key-word argument.
        :type attach_response: bool, optional

        :return: loaded waveform data stream
        :rtype: obspy.Stream
        """        
        if starttime > endtime:
            raise ValueError('starttime is after endtime')
        
        query_str = 'SELECT byteoffset, bytes, filename FROM tsindex WHERE '
        _qs = []
        for _k, _v in zip(['network','station','location','channel'], [network, station, location, channel]):
            # Handle wildcards first
            _haswild = False
            if '?' in _v or '*' in _v:
                _v = wildcard_mapper(_v)
                _haswild = True
            # Handle Comma Delimited strings
            if ',' in _v:
                _v = _v.split(',')
                _u = []
                for _w in _v:
                    # Handle individual wildcard instances
                    if '%' in _w or '_' in _w:
                        _w = f"{_k} LIKE '{_w}'"
                    # Handle individual explicit instances
                    else:
                        _w = f"{_k} = '{_w}'"
                    _u.append(_w)
                # Join comma-delimited with OR
                _q = '(' + ' OR '.join(_u) + ')'
            # Handle Non-Comma Delimited With Wildcard
            elif _haswild:
                _q = f"{_k} LIKE '{_v}'"
            # Handle Non-Comma Delimited Explicit
            else:
                _q = f"{_k} = '{_v}'"
            
            # Append to AND set
            _qs.append(_q)

        # Parse starttime
        if isinstance(starttime, (str, obspy.UTCDateTime)):
            _qs.append(f'endtime>"{starttime}"')
        else:
            raise TypeError('starttime must be type str or UTCDateTime')
        
        # Parse endtime
        if isinstance(endtime, (str, obspy.UTCDateTime)):
            _qs.append(f'starttime<"{endtime}"')
        else:
            raise TypeError('endtime must be type str or UTCDateTime')
        
        if isinstance(quality, str):
            _qs.append(f'quality = "{quality}"')

        # Join all AND's
        _qst = ' AND '.join(_qs)
        # Attach to query string
        query_str += _qst

        rst = self._cursor.execute(query_str)
        st = obspy.Stream()

        for _r in rst:
            byteoffset, byte, seedfile = _r
            with open(seedfile, 'rb') as f:
                f.seek(byteoffset)
                buff = io.BytesIO(f.read(byte))
                try:
                    st += obspy.read(buff,starttime=starttime, endtime=endtime)
                except:
                    breakpoint()

        if minimumlength is None:
            pass
        else:
            raise NotImplementedError('minimumlength placeholder - not used')
        
        if longestonly is None:
            pass
        else:
            raise NotImplementedError('longestonly placeholder - not used')


        if attach_response:
            if 'inventory' in kwargs.keys():
                if isinstance(kwargs['inventory'], obspy.Inventory):
                    st.attach_response(kwargs['inventory'])
                elif isinstance(kwargs['inventory'], (str, Path)):
                    try:
                        inv = obspy.read_inventory(kwargs['inventory'])
                    except Exception as e:
                        warnings.warn(f"inventory={kwargs['inventory']} error: {e}")
                        
                    if inv:
                        st.attach_response(inv)
                else:
                    warnings.warn(f"inventory={kwargs['inventory']} does not look like an Inventory or StationXML file")
                
        if filename is None:
            pass
        else:
            try:
                st.write(filename=filename)
            except Exception as e:
                warnings.warn(f'Attempt to write to "{filename}" failed: {e}')
        
        return st



    def get_waveforms_bulk(self, bulk, **kwargs):
        """
        Follow the API of obspy.clients.fdsn.client.Client.get_waveforms_bulk
        """
        if isinstance(bulk, list):
            st = obspy.Stream()
            if 'filename' in kwargs.keys():
                fn = kwargs.pop('filename')
            else:
                fn = False

            for _b in bulk:
                try:
                    _st = self.get_waveforms(*_b, **kwargs)
                    st += _st
                except Exception as e:
                    warnings.warn(f'failed to run request {_b}: {e}')
                    continue
        if fn:
            try:
                st.write(fn)
            except Exception as e:
                warnings.warn(f'failed to save bulk stream to "{fn}": {e}')
                pass
        return st

