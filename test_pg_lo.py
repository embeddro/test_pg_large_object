#!/usr/bin/env python
import os.path

import psycopg2
from psycopg2.extensions import lobject
from os import listdir
from os.path import isdir, isfile, join, getsize
import time
import fire



class PGBenchmark(object):
    def __init__(self, dbname, user, password, host, port):
        self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        self.cur = self.conn.cursor()
        self.cur.execute("""CREATE TABLE IF NOT EXISTS test_pg_lo (
                            file_path varchar NOT NULL,
                            "oid" int4 NOT NULL,
                            write_to_db_time int4 NULL,
                            read_from_db_time int4 NULL,
                            file_size int4 NULL
                            );""")

    def from_file_to_db(self, path):
        oid = -1
        with open(path, 'rb') as fd:
            try:
                start = int(round(time.time() * 1000))
                lobj = self.conn.lobject(0, 'w', 0)
                oid = lobj.oid
                lobj.write(fd.read())
                lobj.close()
                self.conn.commit()
                end = int(round(time.time() * 1000))
                file_size = getsize(path)
                self.cur.execute("INSERT INTO test_pg_lo (file_path, oid, write_to_db_time, file_size) VALUES (%s, %s, %s, %s);", (os.path.basename(path), oid, (end - start), file_size))
                self.conn.commit()
            except (psycopg2.Warning, psycopg2.Error) as e:
                print("Exception: {}".format(e))
        return oid

    def from_db_to_file(self, oid, path):
        try:
            start = int(round(time.time() * 1000))
            lobj = self.conn.lobject(oid, 'r')
            lobj.export(path)
            lobj.close()
            self.conn.commit()
            end = int(round(time.time() * 1000))
            self.cur.execute("UPDATE test_pg_lo SET read_from_db_time = %s WHERE oid=%s;", ((end - start), oid))
            self.conn.commit()
        except (psycopg2.Warning, psycopg2.Error) as e:
            print("Exception: {}".format(e))

    def read_from_folder_to_db(self, path):
        if isdir(path):
            files = [f for f in listdir(path) if isfile(join(path, f))]
            for file in files:
                self.from_file_to_db(join(path,file))


    def write_from_db_to_folder(self, path):
        if isdir(path):
            try:
                self.cur.execute('SELECT oid, file_path FROM test_pg_lo;')
                file_records = self.cur.fetchall()
                for rec in file_records:
                    self.from_db_to_file(rec[0], join(path, rec[1]))
            except (psycopg2.Warning, psycopg2.Error) as e:
                print("Exception: {}".format(e))

    def clear_all_large_object(self):
        try:
            self.cur.execute('SELECT lo_unlink(l.oid) FROM pg_largeobject_metadata l;')
            self.conn.commit()
        except (psycopg2.Warning, psycopg2.Error) as e:
            print("Exception: {}".format(e))

    def clear_file_table(self):
        try:
            self.cur.execute('DELETE FROM test_pg_lo;')
            self.conn.commit()
        except (psycopg2.Warning, psycopg2.Error) as e:
            print("Exception: {}".format(e))

    def get_lowest_write(self):
        try:
            self.cur.execute('SELECT * FROM test_pg_lo WHERE write_to_db_time = (SELECT max(write_to_db_time) from test_pg_lo);')
            res = self.cur.fetchone()
            self.conn.commit()
        except (psycopg2.Warning, psycopg2.Error) as e:
            print("Exception: {}".format(e))
        return res

    def get_lowest_read(self):
        try:
            self.cur.execute('SELECT * FROM test_pg_lo WHERE read_from_db_time = (SELECT max(read_from_db_time) from test_pg_lo);')
            res = self.cur.fetchone()
            self.conn.commit()
        except (psycopg2.Warning, psycopg2.Error) as e:
            print("Exception: {}".format(e))
        return res

    def get_fastest_write(self):
        try:
            self.cur.execute('SELECT * FROM test_pg_lo WHERE write_to_db_time = (SELECT min(write_to_db_time) from test_pg_lo);')
            res = self.cur.fetchone()
            self.conn.commit()
        except (psycopg2.Warning, psycopg2.Error) as e:
            print("Exception: {}".format(e))
        return res

    def get_fastest_read(self):
        try:
            self.cur.execute('SELECT * FROM test_pg_lo WHERE read_from_db_time = (SELECT min(read_from_db_time) from test_pg_lo);')
            res = self.cur.fetchone()
            self.conn.commit()
        except (psycopg2.Warning, psycopg2.Error) as e:
            print("Exception: {}".format(e))
        return res

    def get_average_write(self):
        try:
            self.cur.execute('SELECT AVG(write_to_db_time) FROM test_pg_lo;')
            res = self.cur.fetchone()
            self.conn.commit()
        except (psycopg2.Warning, psycopg2.Error) as e:
            print("Exception: {}".format(e))
        return int(res[0])

    def get_average_read(self):
        try:
            self.cur.execute('SELECT AVG(read_from_db_time) FROM test_pg_lo;')
            res = self.cur.fetchone()
            self.conn.commit()
        except (psycopg2.Warning, psycopg2.Error) as e:
            print("Exception: {}".format(e))
        return int(res[0])

    def get_total_size(self):
        try:
            self.cur.execute('SELECT SUM(file_size) FROM test_pg_lo;')
            res = self.cur.fetchone()
            self.conn.commit()
        except (psycopg2.Warning, psycopg2.Error) as e:
            print("Exception: {}".format(e))
        return int(res[0]/1000)


    def delete_meta_table(self):
        try:
            self.cur.execute('DROP TABLE IF EXISTS test_pg_lo')
            self.conn.commit()
        except (psycopg2.Warning, psycopg2.Error) as e:
            print("Exception: {}".format(e))

def start(dbname='test_largeobject', user='tester', host='localhost', password='tester', port=5432):
    pgb = PGBenchmark(dbname=dbname, user=user, host=host, password=password, port=port)
    pgb.clear_all_large_object()
    pgb.clear_file_table()
    start_w = int(round(time.time() * 1000))
    pgb.read_from_folder_to_db('./input')
    end_w = int(round(time.time() * 1000))
    start_r = int(round(time.time() * 1000))
    pgb.write_from_db_to_folder('./output')
    end_r = int(round(time.time() * 1000))
    stat = pgb.get_lowest_write()
    print(f"Самая медленная запись в базу: файл - {stat[0]}, размер - {int(stat[4]/1000)} kB,  время - {stat[2]} ms")
    stat = pgb.get_fastest_write()
    print(f"Самая быстрая запись в базу: файл - {stat[0]}, размер - {int(stat[4]/1000)} kB, время - {stat[2]} ms")
    stat = pgb.get_average_write()
    print(f"Среднее время записи в базу: {stat} ms")
    stat = pgb.get_lowest_read()
    print(f"Самое медленное чтение из базы: файл - {stat[0]}, размер - {int(stat[4]/1000)} kB, время - {stat[3]} ms")
    stat = pgb.get_fastest_read()
    print(f"Самое быстрое чтение из базы: файл - {stat[0]}, размер - {int(stat[4]/1000)} kB, время - {stat[3]} ms")
    stat = pgb.get_average_read()
    print(f"Среднее время чтения из базы: {stat} ms")
    print(f"Всего записано в базу: {pgb.get_total_size()} kB за {end_w - start_w} ms")
    print(f"Чтение из базы и запись на диск всей папки: {pgb.get_total_size()} kB за {end_r - start_r} ms")
    pgb.clear_all_large_object()
    pgb.delete_meta_table()



if __name__ == '__main__':
    fire.Fire(start)

