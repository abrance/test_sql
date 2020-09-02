

-- 创建用户 test，密码是 anyun100
CREATE USER 'test'@'localhost' identified by 'anyun100';

-- 创建数据库，存在则不创建
CREATE DATABASE IF NOT EXISTS test CHARACTER SET utf8mb4;

-- 将数据库 test 的所有权限都授权给用户 test
GRANT ALL ON test.* to 'test'@'localhost';
GRANT ALL ON test.* to 'test'@'%';

-- 允许 test 用户远程登录
GRANT ALL PRIVILEGES ON test.* to 'test'@'%' IDENTIFIED BY 'anyun100' WITH GRANT OPTION;
FLUSH PRIVILEGES;
