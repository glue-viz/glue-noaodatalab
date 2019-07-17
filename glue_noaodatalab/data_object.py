from dl import queryClient as qc

import numpy as np

from glue.core.component_id import ComponentID
from glue.core.data import BaseCartesianData
from glue.core.subset import RangeSubsetState

# For now, have hard coded table size
LIMIT = 0.01

SQL_TABLE_INFO = """
SELECT COLUMN_NAME,DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}'
""".strip()

SQL_SINGLE_COLUMN = """
SELECT {column} FROM {full_table} WHERE random_id < {limit}
""".strip()

# The 1000000 checks are to filter out NaN/Inf values
SQL_STATISTIC = """
SELECT {sql_func}({column})
    FROM (SELECT {column} FROM {full_table} WHERE random_id < {limit} AND {column} > -1000000 AND {column} < 1000000) as t;
""".strip()

SQL_HISTOGRAM1D = """
SELECT width_bucket({column}, {min}, {max}, {nbin}) as bucket,
        count(*) as cnt
    FROM (SELECT * FROM {full_table} WHERE random_id < {limit} {where}) as t
group by bucket
order by bucket;
""".strip()


class NOAOSQLData(BaseCartesianData):

    def __init__(self, full_table):

        super(NOAOSQLData, self).__init__()

        # TODO: shouldn't need this!
        self.coords = None

        self._full_table = full_table

        # Get column names and types
        schema, table = full_table.split('.')
        query = SQL_TABLE_INFO.format(schema=schema, table=table)

        result = self._query_sql(query)
        columns = list(zip(result['column_name'], result['data_type']))

        # For now only keep numerical columns
        columns = [item for item in columns if ('double' in item[1] or 'real' in item[1])]

        self._columns = [item[0] for item in columns]

        self._cids = [ComponentID(label=name, parent=self) for name in self._columns]

        self._kind = {name: 'numerical' for name in self._columns}

    @property
    def label(self):
        return self._full_table

    @property
    def shape(self):
        return (LIMIT,)

    @property
    def main_components(self):
        return self._cids

    def get_kind(self, cid):
        print('get_kind')
        return self._kind[cid.label]

    def get_data(self, cid, view=None):
        print('get_data')
        if cid in self.pixel_component_ids:
            return super(NOAOSQLData, self).get_data(cid, view=view)
        else:
            query = SQL_SINGLE_COLUMN.format(column=cid.label, full_table=self._full_table, limit=LIMIT)
            result = self._query_sql(query)
            if view is None:
                view = Ellipsis
            return np.array(result[cid.label])[view]

    def get_mask(self, subset_state, view=None):
        return subset_state.to_mask(self, view=view)

    def compute_statistic(self, statistic, cid,
                          axis=None, finite=True,
                          positive=False, subset_state=None,
                          percentile=None, random_subset=None):
        print('compute_statistic')
        if axis is None:
            if statistic == 'minimum':
                if cid in self.pixel_component_ids:
                    return 0
                else:
                    sql_func = 'MIN'
            elif statistic == 'maximum':
                if cid in self.pixel_component_ids:
                    return self.shape[cid.axis]
                else:
                    sql_func = 'MAX'
            elif statistic == 'mean':
                sql_func = 'AVG'
            elif statistic == 'median':
                raise NotImplementedError()
            elif statistic == 'percentile':
                raise NotImplementedError()
            elif statistic == 'sum':
                sql_func = 'SUM'
            query = SQL_STATISTIC.format(sql_func=sql_func, column=cid.label, full_table=self._full_table, limit=LIMIT)

            result = self._query_sql(query)
            return float(result[sql_func.lower()])
        else:
            raise NotImplementedError()

    def compute_histogram(self, cid,
                          range=None, bins=None, log=False,
                          subset_state=None, subset_group=None):
        print("compute_histogram", cid, range, bins)
        try:
            if len(bins) == 1:
                min, max = range[0]
                nbin = bins[0]
                if subset_state is None:
                    where = ""
                elif isinstance(subset_state, RangeSubsetState):
                    where = "AND {column} > {min} AND {column} < {max}".format(column=subset_state.att.label,
                                                                                 min=subset_state.lo,
                                                                                 max=subset_state.hi)
                else:
                    raise NotImplementedError()
                query = SQL_HISTOGRAM1D.format(column=cid[0].label, full_table=self._full_table,
                                               limit=LIMIT, min=min, max=max, nbin=nbin, where=where)
                result = self._query_sql(query)
                bucket = np.array(result['bucket'])
                values = np.array(result['cnt'])
                keep = (bucket >= 0) & (bucket < nbin)
                histogram = np.zeros(nbin)
                histogram[bucket[keep]] = values[keep]
                return histogram
            else:
                raise NotImplementedError()
        except:
            import traceback
            traceback.print_exc()
            raise

    def _query_sql(self, query):
        print('-' * 72)
        print("QUERY")
        print(query)
        result = qc.query(query, fmt='pandas')
        print("RESULT")
        print(result)
        return result
# query = 'SELECT ra,dec,flux_r FROM ls_dr6.tractor LIMIT 10'
# result = self._query_sql(query)
# print(result)
#
# query = """
# SELECT width_bucket(flux_r, 0, 100, 10) as bucket,
#         count(*) as cnt
#     FROM (SELECT flux_r FROM ls_dr6.tractor LIMIT 1000000) as t
# group by bucket
# order by bucket;
# """.strip()
# result = self._query_sql(query)
# print(result)
#
#
#
# # query = 'SELECT ra,dec FROM ls_dr6.tractor LIMIT 100'
# # result = self._query_sql(query)
#
# print(result)

# PostGres 10
# qtreec
# ls6_dr6.tractor
# ls6_dr6.tractor_cs
# nest4096
# ring256
# random_id
