from .DBcm import UseDatabase


def update_db(table, id, update_map, config, id_key='id', logger=None):
    logger.debug(f"[update_db][{id}] update_map={update_map}, table={table}")
    if (len(update_map) > 0):
        with UseDatabase(config) as cursor:
            setVal = ''

            for key, value in update_map.items():
                setVal += f"{key} = '{value}',"

            _SQL = f"""UPDATE {table} SET {setVal[:-1]} WHERE {id_key} = {id}"""

            logger.debug(f"[update_db][{id}] sql cmd={_SQL}")
            cursor.execute(_SQL)

def bulk_update_height_width_db(config, data, table='recording', logger=None):
    logger.debug(f"[bulk_update_height_width_db] len_data={len(data)}, table={table}")
    if (len(data) > 0):
        with UseDatabase(config) as cursor:
            # _SQL = f"""INSERT INTO {table} (id, width, height) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE id=VALUES(id), width=VALUES(width), height=VALUES(height)"""
            _SQL = f"""UPDATE {table} SET width = %s, height = %s WHERE id = %s"""
            # logger.debug(f"[bulk_update_height_width_db] sql cmd={_SQL}")
            cursor.executemany(_SQL, data)


def insert_song_db(data, config, logger=None):
    logger.debug(f"[insert_db] data={data}")
    with UseDatabase(config) as cursor:

        _SQL = """INSERT INTO song (`name`, cover, duration, url, deleted, status, tag_id, tag_name, create_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        logger.debug(f"[insert_db] sql cmd={_SQL}")
        cursor.execute(_SQL, data)
        logger.debug(f"[insert_db] cursor.lastrowid={cursor.lastrowid}")

    return cursor.lastrowid


""" Get cover_start_time and url by recording_id
:param str table: ex: recording
:param str id   : ex: 123456789012345678
:param str config : ex: {'host':'127.0.0.1',...}
:param str logger: ex: None
:rtype res: ('cover_start_time', 'url)
"""


def get_values_from_db(table, id, config, fields, id_key='id', logger=None):
    logger.debug(f"[get_values_from_db][{id}] fields={fields}, table={table}")
    if (len(fields) > 0):
        with UseDatabase(config) as cursor:
            getField = ''

            for key in fields:
                getField += f"{key},"
            _SQL = f"""SELECT {getField[:-1]} FROM {table} WHERE {id_key} = {id}"""
            logger.debug(f"[get_values_from_db][{id}] sql cmd={_SQL}")
            cursor.execute(_SQL)
            res = cursor.fetchone()
            logger.debug(f"[get_values_from_db][{id}] res={res}")

    return res

def get_values_from_db_batch(table, ids, config, fields, id_key='id', logger=None):
    logger.debug(f"[get_values_from_db] len_ids={len(ids)}, fields={fields}, table={table}")
    res = ()
    if (len(fields) > 0 and len(ids) > 0):
        with UseDatabase(config) as cursor:
            getField = ''
            for key in fields:
                getField += f"{key},"
            ridList = str(ids)[1:-1]
            _SQL = f"""SELECT {getField[:-1]} FROM {table} WHERE {id_key} IN ({ridList})"""
            logger.debug(f"[get_values_from_db] sql cmd={_SQL}")
            cursor.execute(_SQL)
            res = cursor.fetchall()
            logger.debug(f"[get_values_from_db] res={res}")
    return res


def get_todo_watermark_rid(config, table='jungle2_watermark', logger=None):
    logger.debug(f"[get_todo_watermark] table={table}")
    with UseDatabase(config) as cursor:
        _SQL = f"""SELECT recording_id, url, dbo_url FROM {table} WHERE watermark_video_url IS NULL OR watermark_video_url = ''"""
        # logger.debug(f"[get_todo_watermark] sql cmd={_SQL}")
        cursor.execute(_SQL)
        res = cursor.fetchall()
    return res

def get_todo_video_info_rid(config, failed_rids=[], table='recording', logger=None, limit=300):
    logger.debug(f"[get_todo_video_info_rid] table={table}, len_failed_rids={len(failed_rids)}")
    res = ()
    with UseDatabase(config) as cursor:
        _SQL = f"""SELECT id, url, dbo_url, width, height FROM {table} WHERE url IS NOT NULL AND url != '' AND (height=0 OR width=0) AND deleted=0 ORDER BY create_time DESC LIMIT {limit}"""

        if len(failed_rids) > 0:
            f_rids = str(failed_rids)[1:-1]
            _SQL = f"""SELECT id, url, dbo_url, width, height FROM {table} WHERE id NOT IN ({f_rids}) AND url IS NOT NULL AND url != '' AND (height=0 OR width=0) AND deleted=0 ORDER BY create_time DESC LIMIT {limit}"""

        logger.debug(f"[get_todo_video_info_rid] sql cmd={_SQL}")
        cursor.execute(_SQL)
        res = cursor.fetchall()
    return res


