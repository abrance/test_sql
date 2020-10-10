# -*- coding: utf-8 -*-

import datetime
import sys

import contextlib
import sqlalchemy
import sqlalchemy.ext
import sqlalchemy.ext.declarative
import sqlalchemy.dialects.mysql
from sqlalchemy.sql import func
from sqlalchemy.orm import sessionmaker


mds_conn_str = "mysql+pymysql://root:123456@127.0.0.1:3306/test?charset=utf8mb4"
OK = 10
Base = sqlalchemy.ext.declarative.declarative_base()


class Test(Base):
    __tablename__ = 'test1'
    mysql_charset = "utf8mb4"

    s = sqlalchemy.Column(sqlalchemy.String(32), primary_key=True)
    i = sqlalchemy.Column(sqlalchemy.INT)  # 2020/3/31 客户端标识
    i2 = sqlalchemy.Column(sqlalchemy.INT)
    create_time = sqlalchemy.Column(sqlalchemy.DATETIME, default=datetime.datetime.now())



class RecordLog(Base):
    __tablename__ = "record_log"
    mysql_charset = "utf8mb4"
    # 2020/9/21 按照一个目录只能归档一遍的原则，将目录名设置为主键，当有覆盖时，将取消掉上次的归档
    # 2020/9/21 这些是目录本身的属性， store_year决定了 pool 和 pool_no；规则可以制定
    id = sqlalchemy.Column(sqlalchemy.INT, nullable=False, primary_key=True, autoincrement=True)
    dir_name = sqlalchemy.Column(sqlalchemy.String)

    pool = sqlalchemy.Column(sqlalchemy.String)
    pool_no = sqlalchemy.Column(sqlalchemy.String)
    store_year = sqlalchemy.Column(sqlalchemy.INT)

    dir_level = sqlalchemy.Column(sqlalchemy.INT, nullable=False)
    dir_type = sqlalchemy.Column(sqlalchemy.String)
    store_time = sqlalchemy.Column(sqlalchemy.INT)
    case_no = sqlalchemy.Column(sqlalchemy.INT)

    # 2020/9/21 非归档目录的归档目录名是 上层目录，归档目录的是它自己
    archived_name = sqlalchemy.Column(sqlalchemy.String)

    # 2020/9/21 下面这俩只有归档目录才会有
    leaf_dirs = sqlalchemy.Column(sqlalchemy.TEXT)
    pool_no_ls = sqlalchemy.Column(sqlalchemy.String)



    
# class FileInfo(Base):
#     """这是一张大表，每个文件有一行记录，大约有一亿条以上的记录

#     这张表记录了文件的路径散列值，文件的内容散列值，文件的实际存放位置，
#     文件的元信息

#     """
#     __tablename__ = 'fileinfo1'
#     mysql_charset = "utf8mb4"

#     fileid = sqlalchemy.Column(sqlalchemy.String(32), nullable=False, primary_key=True)
#     agent_id = sqlalchemy.Column(sqlalchemy.INT, nullable=False, name='agentid', primary_key=True)
#     dir_id = sqlalchemy.Column(sqlalchemy.INT, nullable=False)
#     filename = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)  # 2020/3/31 文件名
#     file_suffix = sqlalchemy.Column(sqlalchemy.String(10), default=None, name='filesuffix')  # 2020/3/31 后缀名
#     file_md5 = sqlalchemy.Column(sqlalchemy.String(32), nullable=False, name='filemd5')  # 2020/3/31 文件 md5 散列值
#     file_state = sqlalchemy.Column(sqlalchemy.SmallInteger, name='filestate')  # 2020/3/31 文件状态
#     file_size = sqlalchemy.Column(sqlalchemy.BIGINT, nullable=False, name='filesize')  # 2020/3/31 文件大小,B
#     create_time = sqlalchemy.Column(sqlalchemy.DATETIME, nullable=False, primary_key=True)  # 2020/3/31 文件创建时间
#     last_modify_time = sqlalchemy.Column(sqlalchemy.DATETIME, nullable=True)  # 2020/3/31 最近修改时间
#     is_beg = sqlalchemy.Column(sqlalchemy.SMALLINT, default=False, nullable=False)  # 2020/3/31 是否申请粉碎
#     beg_time = sqlalchemy.Column(sqlalchemy.DATETIME)  # 2020/3/31 申请粉碎时间
#     is_add = sqlalchemy.Column(sqlalchemy.Boolean, default=False, nullable=False)  # 区分扫描上传的和逐个上传
#     # 2020/8/26 sdm扫描记录放这里
#     is_scan = sqlalchemy.Column(sqlalchemy.SmallInteger, default=0, nullable=False)


class DataBase(object):
    def __init__(self):
        self.engine = sqlalchemy.create_engine(
            mds_conn_str,
            # echo=True,
            pool_recycle=3600
        )
        self.Session = sessionmaker(self.engine)
        try:
            self.create_tables()
        except Exception as e:
            print("database initialize failed")
            print("Exception: {}".format(e))
            sys.exit(-1)
        else:
            # 创建数据库表成功，继续往下执行
            pass

    def create_tables(self):
        Base.metadata.create_all(self.engine)

    def destroy_session(self):
        self.engine.dispose()

    @contextlib.contextmanager
    def session_scope(self):
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            print("exception occurs: {}, {}".format(type(e), e))
            if type(e) is sqlalchemy.exc.IntegrityError:
                ecode = e.orig.args[0]
                if ecode == 1062:  # Duplicate key
                    raise Exception
                else:
                    session.rollback()
                    print("> session commit failed 1, rollback")
                    raise Exception
            else:
                session.rollback()
                print("> session commit failed 2, rollback")
                raise Exception
        finally:
            session.close()

    # 使用 统计
    # def query_agent_upload(self):
    #     # 
    #     print('<<<< query_agent_upload ')
    #     with self.session_scope() as session:
    #         query = session.query(FileInfo.agent_id, func.count(FileInfo.fileid)).filter(
    #             FileInfo.file_state == OK
    #         ).group_by(FileInfo.agent_id).order_by(FileInfo.agent_id)
    #         print(query.all())
    #         # [(1000, 739)]

    # def date_type(self):
    #     # 测试date类型
    #     today = datetime.date.today()
    #     with self.session_scope() as session:
    #         query = session.query()

    def _substring_index(self):
        # 使用 func.substring_index
        string = 'keyword'
        with self.session_scope() as session:
            query_t = session.query(Test).filter(
                func.substring_index(Test.s, '/', -1).like('%'+string+'%')
            )
            row_t = query_t.first()
            for row_t in query_t.all():
                cnt = query_t.count()
                print((row_t.s, row_t.i, row_t.i2), '\n', cnt)
            return True

    def _order_by_multi(self):
        # 多个字段 规则排序
        with self.session_scope() as session:
            query_t = session.query(Test).order_by(Test.i, Test.create_time.desc())
            for row_t in query_t.all():
                print((row_t.s, row_t.i, row_t.i2, row_t.create_time))
            return True

    def _ret_single_value(self):
        # 这里是返回不了单个值的，而是返回一个元组(1, ) 需要取值，有一点麻烦
        with self.session_scope() as session:
            query_t = session.query(Test.i).first()
            # _query_t = session.query(Test.i).first()[0]
            print(query_t)
            # print(_query_t)
            return True

    def _ret_timestamp(self):
        # 这里是返回不了单个值的，而是返回一个元组(1, ) 需要取值，有一点麻烦
        with self.session_scope() as session:
            query_t = session.query(Test).first()
            print(query_t.create_time.timestamp())
            # _query_t = session.query(Test.i).first()[0]
            # print(query_t)
            # print(_query_t)
            return True

    def _compare_datetime_type(self):
        # 支持mysql datetime类型与datetime类型直接比较
        with self.session_scope() as session:
            query_t = session.query(Test).filter(
                Test.create_time < datetime.datetime.now()
            )
            row_t = query_t.first()
            if row_t:
                print(row_t.create_time)
                return True
            else:
                return False

    def _row_attr_to_dc(self):
        # 试试让row的属性变为字典, 测试是可行的
        with self.session_scope() as session:
            query_t = session.query(Test).first()
            
            query_t.__dict__.pop('_sa_instance_state', -1)
            d1 = {}
            d1.update(query_t.__dict__)
            i = d1.get('i')
            print(i)
            print(query_t.__dict__)
            
            return True
        
    def save_record_cache_to_db(self, dir_name, info: dict):
        print('<<<< save_record_cache_to_db dir_name: {} info: {}'.format(dir_name, info))
        try:
            with self.session_scope() as session:
                cnt = session.query(func.count(RecordLog.id)).filter(
                    RecordLog.dir_name == dir_name
                ).scalar()
                # 2020/9/22 修改info里的leaf_dirs和pool_no_ls，列表转为字符串
                leaf_dirs = info.get('leaf_dirs')
                pool_no_ls = info.get('pool_no_ls')
                if isinstance(leaf_dirs, list):
                    str_leaf_dirs = ','.join(leaf_dirs)
                    info.__setitem__('leaf_dirs', str_leaf_dirs)
                if pool_no_ls:
                    pool_no_ls = ','.join(pool_no_ls)
                    info.__setitem__('pool_no_ls', pool_no_ls)
                v = info.pop('disk_no')
                info.__setitem__('pool_no', v)
                if cnt == 0:
                    row_rl = RecordLog(**info)
                    print('<<<< save_record_cache_to_db dir_name: {}'.format(dir_name))
                    session.add(row_rl)
                    return True
                elif cnt == 1:
                    # 2020/9/21 如果已经归档，如果还未归档两种情况
                    session.query(RecordLog).filter(
                        RecordLog.dir_name == dir_name
                    ).update(info)
                    print('<<<< save_record_cache_to_db update dir_name')
                    return True
                else:
                    print('<<<< record log dir_name duplicate: {}'.format(dir_name))
                    return False
        except Exception as e:
            print('<<<< save_record_cache_to_db error {}'.format(e))
            return False


    def test_group_by(self):
        with self.session_scope() as session:
            query_t = session.query(Test.i).group_by(Test.i)
            
            print([i[0] for i in query_t.all()])
            
            return True
        
db = DataBase()


if __name__ == '__main__':
    # db.query_agent_upload()
    # db._order_by_multi()
    # db._ret_single_value()
    # db._ret_timestamp()
    # db._compare_datetime_type()
    # db._row_attr_to_dc()
    # db.save_record_cache_to_db('/data/303',  {'dir_name': '/data/303', 'dir_level': 2, 'store_year': 0, 'dir_type': '', 'store_time': -1, 'case_no': '', 'disk_no': '', 'pool_no_ls': ['POOL-20200717090945'], 'leaf_dirs': ['/data/303/303-2020/303-2020-30']})
    db.test_group_by()
    
