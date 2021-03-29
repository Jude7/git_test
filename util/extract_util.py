# -*- coding: utf-8 -*-

from config import HIVE_PORT, HIVE_IP
import sqlparse


def extract2(in_sql, db_name, host=HIVE_IP, job_limit=True):
    """
    :return None
    :param in_sql:
    :return:
    """
    from impala.dbapi import connect

    conn = connect(host=host,
                   port=HIVE_PORT,
                   auth_mechanism='PLAIN',
                   user='hadoop',
                   database=db_name, timeout=1200)
    print(sqlparse.format(in_sql, reindent=True, keyword_case='upper'))

    cur = conn.cursor()
    cur.execute("set hive.exec.dynamic.partition.mode=nonstrict")
    cur.execute("SET hive.mapred.mode=nonstrict")
    cur.execute("set hive.merge.mapfiles=true")
    cur.execute("set hive.merge.mapredfiles=true")
    if job_limit:
        cur.execute("set hive.merge.smallfiles.avgsize = 134217728")
        cur.execute("set hive.merge.size.per.task=209715200")
        cur.execute("set mapreduce.job.reduces=1")
    # cur.execute("set mapred.max.split.size=68157440")
    # cur.execute("set mapred.min.split.size=68157440")
    # cur.execute("set hive.exec.mode.local.auto=true")
    # cur.execute("set hive.exec.parallel= true")
    # cur.execute("set hive.exec.compress.intermediate=true")
    # cur.execute("set hive.input.format = org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
    # cur.execute("set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec")
    # cur.execute("set hive.intermediate.compression.type=BLOCK")
    # cur.execute("set hive.map.aggr=true")

    cur.execute(in_sql)
    cur.close()
    return "Done"


def extract(in_sql, db_name, host=HIVE_IP, mapjoin=True):
    """

    :rtype: dataframe
    """
    from impala.util import as_pandas
    from impala.dbapi import connect

    conn = connect(host=host, port=HIVE_PORT,
                   user='hadoop',
                   auth_mechanism='PLAIN',
                   database=db_name, timeout=1200)
    print(db_name)
    print(sqlparse.format(in_sql, reindent=True, keyword_case='upper'))

    cur = conn.cursor()
    cur.execute("set hive.exec.dynamic.partition.mode=nonstrict")
    cur.execute("SET hive.mapred.mode=nonstrict")
    cur.execute("set hive.merge.mapfiles=true")
    cur.execute("set hive.merge.mapredfiles=true")
    cur.execute("set hive.merge.smallfiles.avgsize = 134217728")
    cur.execute("set hive.merge.size.per.task=209715200")
    cur.execute("set mapred.max.split.size=68157440")
    cur.execute("set mapred.min.split.size=68157440")
    if not mapjoin:
        cur.execute("set hive.auto.convert.join=false")

    # cur.execute("set hive.exec.mode.local.auto=true")
    # cur.execute("set hive.exec.parallel= true")
    # cur.execute("set hive.exec.compress.intermediate=true")
    # cur.execute("set hive.input.format = org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
    # cur.execute("set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec")
    # cur.execute("set hive.intermediate.compression.type=BLOCK")
    # cur.execute("set hive.map.aggr=true")
    """
    set mapred.max.split.size=100000000;
    set mapred.min.split.size.per.node=1000000;
    set mapred.min.split.size.per.rack=1000000;
    set mapreduce.job.maps=128; force to 128
    """
    cur.execute(in_sql)
    df = as_pandas(cur)
    cur.close()
    return df

