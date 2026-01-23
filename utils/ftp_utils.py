import paramiko
import os
import logging
from stat import S_ISDIR
from typing import Optional, List, Dict, Callable
from datetime import datetime


class SFTPClient:
    """SFTP 客户端工具类"""

    def __init__(self,
                 host: str,
                 username: str,
                 password: Optional[str] = None,
                 port: int = 22,
                 logger: logging.Logger = None,
                 key_path: Optional[str] = None):
        """
        初始化SFTP连接
        :param host: 服务器地址
        :param username: 用户名
        :param password: 密码（密码或密钥二选一）
        :param port: 端口，默认22
        :param key_path: 密钥文件路径
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.key_path = key_path
        self.transport = None
        self.sftp = None
        self.logger = logger if logger else logging.getLogger('SFTPClient')
        self._connect()

    def _connect(self):
        """建立SFTP连接"""
        try:
            self.transport = paramiko.Transport((self.host, self.port))

            # 认证方式选择
            if self.key_path:
                private_key = paramiko.RSAKey.from_private_key_file(self.key_path)
                self.transport.connect(username=self.username, pkey=private_key)
            else:
                self.transport.connect(username=self.username, password=self.password)

            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
        except Exception as e:
            self.logger.error(f"连接失败: {str(e)}")
            raise

    def check_sftp_path_exists(self, path):
        """
        检查SFTP路径是否存在
        :param sftp: paramiko.SFTPClient 实例
        :param path: 要检查的路径
        :return: True 如果路径存在，否则 False
        """
        try:
            # 获取路径属性
            self.sftp.stat(path)
            return True
        except FileNotFoundError:
            # 路径不存在
            return False
        except Exception as e:
            # 其他异常
            print(f"检查路径时发生错误: {e}")
            return False

    def list_directory(self, remote_path: str) -> List[Dict]:
        """
        列出远程目录内容
        :return: [{
                'name': 文件名,
                'type': 'files'|'dir',
                'size': 字节数,
                'modify': 修改时间
        }]
        """
        try:
            files = []
            for attr in self.sftp.listdir_attr(remote_path):
                files.append({
                    'name': attr.filename,
                    'type': 'dir' if S_ISDIR(attr.st_mode) else 'files',
                    'size': attr.st_size,
                    'modify': datetime.fromtimestamp(attr.st_mtime).isoformat()
                })
            return files
        except IOError as e:
            self.logger.error(f"路径不存在: {remote_path}")
            raise FileNotFoundError(f"远程路径不存在: {remote_path}") from e

    def download_file(self,
                      remote_path: str,
                      local_path: str,
                      progress_callback: Optional[Callable[[int, int], None]] = None):
        """
        下载单个文件
        :param progress_callback: 回调函数 (已传输字节, 总字节)
        """
        start_time = datetime.now()
        # 确保本地目录存在
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # 带进度回调的下载
        self.sftp.get(remotepath=remote_path,
                      localpath=local_path,
                      callback=progress_callback)

        time_cost = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"下载成功: {remote_path} -> {local_path} [{time_cost:.2f}s]")

    def download_directory(self,
                           remote_dir: str,
                           local_dir: str,
                           progress_callback: Optional[Callable[[str, int, int], None]] = None) -> bool:
        """
        递归下载整个目录
        :param progress_callback: 回调函数（当前文件路径，已传输字节，总字节）
        """
        try:
            os.makedirs(local_dir, exist_ok=True)
            for item in self.list_directory(remote_dir):
                remote_path = os.path.join(remote_dir, item['name'])
                local_path = os.path.join(local_dir, item['name'])

                if item['type'] == 'dir':
                    self.download_directory(remote_path, local_path, progress_callback)
                else:
                    # 单个文件进度包装
                    def file_callback(transferred, total):
                        if progress_callback:
                            progress_callback(remote_path, transferred, total)

                    self.download_file(remote_path, local_path, file_callback)
            return True
        except Exception as e:
            self.logger.exception(f"目录下载失败: {str(e)}")
            return False

    def upload_file(self,
                    local_path: str,
                    remote_path: str,
                    progress_callback: Optional[Callable[[int, int], None]] = None) -> bool:
        """
        上传单个文件
        """
        start_time = datetime.now()
        try:
            # 确保远程目录存在
            self._mkdir_p(os.path.dirname(remote_path))

            self.sftp.put(localpath=local_path,
                          remotepath=remote_path,
                          callback=progress_callback)

            time_cost = (datetime.now() - start_time).total_seconds()
            self.logger.info(f"上传成功: {local_path} -> {remote_path} [{time_cost:.2f}s]")
            return True
        except Exception as e:
            self.logger.error(f"上传失败: {str(e)}")
            return False

    def upload_directory(self,
                         local_dir: str,
                         remote_dir: str,
                         progress_callback: Optional[Callable[[str, int, int], None]] = None) -> bool:
        """
        递归上传整个目录
        """
        try:
            self._mkdir_p(remote_dir)

            for item in os.listdir(local_dir):
                local_path = os.path.join(local_dir, item)
                remote_path = os.path.join(remote_dir, item)

                if os.path.isdir(local_path):
                    self.upload_directory(local_path, remote_path, progress_callback)
                else:
                    # 单个文件进度包装
                    def file_callback(transferred, total):
                        if progress_callback:
                            progress_callback(local_path, transferred, total)

                    self.upload_file(local_path, remote_path, file_callback)
            return True
        except Exception as e:
            self.logger.error(f"目录上传失败: {str(e)}")
            return False

    def _mkdir_p(self, remote_path: str):
        """递归创建远程目录"""
        try:
            self.sftp.stat(remote_path)
        except IOError:
            dirname, basename = os.path.split(remote_path.rstrip('/'))
            if dirname:
                self._mkdir_p(dirname)
            try:
                self.sftp.mkdir(remote_path)
            except PermissionError as e:
                self.logger.error(f"创建目录权限不足: {remote_path}")
                raise

    def close(self):
        """关闭连接"""
        if self.sftp:
            self.sftp.close()
        if self.transport:
            self.transport.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
