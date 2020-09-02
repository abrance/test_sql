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
            
            
db = DataBase()


if __name__ == '__main__':
    # db.query_agent_upload()
    db._substring_index()
