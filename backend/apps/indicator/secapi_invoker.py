# -*- coding: utf-8 -*-

import jpype
import jpype.imports
from typing import Optional, Tuple, Any
import sys

from common.utils.utils import SQLBotLogUtil

# 修复Python 3.10+中collections.Sequence的兼容性问题
#if sys.version_info >= (3, 10):
#    from collections.abc import Sequence
#else:
#    from collections import Sequence
from collections.abc import Sequence

class SecAPIInvoker:
    """
    SecAPI 调用封装类，用于调用 cn.ccb.secapi.SecAPI 类的静态方法
    """

    _jvm_started = False
    _sec_init = False
    
    def __init__(self, jar_path: str = "/home/sa/sa/jar/csc-secapi-1.0.8.3-jdk-1.6.jar"):
        """
        初始化 SecAPI 调用器
        
        Args:
            jar_path (str): SecAPI JAR 文件路径
        """

        # 添加JVM启动参数，可能需要更多内存或特定配置
        jvm_args = [
            "-ea",
            f"-Djava.class.path={jar_path}",
            "-Xmx512m"  # 增加JVM内存
        ]
        
        if not SecAPIInvoker._jvm_started:            
            if not jpype.isJVMStarted():
                SQLBotLogUtil.info('启动JVM' + '-'*30)
                jpype.startJVM(
                    jpype.getDefaultJVMPath(),
                    *jvm_args
                )
            SecAPIInvoker._jvm_started = True
        
        self.secapi_class = "cn.ccb.secapi.SecAPI"
        
        # 预加载SecAPI类
        try:
            self.SecAPI = jpype.JClass(self.secapi_class)
            print("√ SecAPI类加载成功")
        except Exception as e:
            print(f"× SecAPI类加载失败: {e}")
            raise

    def node_init(self, sec_node_id: str) -> Tuple[bool, Optional[str]]:
        """
        节点初始化接口
        
        Args:
            sec_node_id (str): 安全节点ID
            
        Returns:
            Tuple[bool, Optional[str]]: (是否成功, 错误信息)
        """
        if SecAPIInvoker._sec_init:
            return True, None
        try:
            j_string = jpype.JString(sec_node_id)
            self.SecAPI.nodeInit(j_string)
            SecAPIInvoker._sec_init = True
            return True, None
        #except jpype._jexception.cn.ccb.secapi.SecExceptionPyRaisable as e:
        #    error_msg = str(e)
        #    print(f"节点初始化错误: {error_msg}")
        #    # 尝试获取错误码（如果SecException包含错误码）
        #    if hasattr(e, 'getErrorCode'):
        #        error_code = e.getErrorCode()
        #        return False, f"错误码: {error_code}, 信息: {error_msg}"
        #    return False, error_msg
        except Exception as e:
            print(f"节点初始化异常: {e}")
            return False, str(e)
    
    def python_bytes_to_java_byte_array(self, in_data: bytes):
        """兼容性字节数组转换"""
        j_byte_array = jpype.JArray(jpype.JByte)(len(in_data))
        for i in range(len(in_data)):
            j_byte_array[i] = in_data[i]
        return j_byte_array
    
    def generate_mac(self, sec_node_id: str, dest_sec_node_id: str, in_data: bytes) -> Tuple[bool, Optional[bytes], Optional[str]]:
        """
        3.6.13 生成MAC
        
        Args:
            sec_node_id (str): 源安全节点ID
            dest_sec_node_id (str): 目标安全节点ID
            in_data (bytes): 输入数据
            
        Returns:
            Tuple[bool, Optional[bytes], Optional[str]]: (是否成功, MAC结果, 错误信息)
        """
        try:
            # 将Python bytes转换为Java byte数组
            print('进入方法-----------------')
            #try:
            #    j_byte_array = jpype.JArray(jpype.JByte)(in_data)
            #except Exception:
            #    # 方法2：使用兼容性转换
            #    j_byte_array = self.python_bytes_to_java_byte_array(in_data)
            j_byte_array = in_data.encode('utf-8')
            print(f"待加密的报文：{j_byte_array}")
            print(f"待加密的报文类型：{type(j_byte_array)}")
        
            print('*'*60)
            print('开始调用')
            print('*'*60)
            
            # 调用mac方法
            result = self.SecAPI.mac(sec_node_id, dest_sec_node_id, j_byte_array)
            print('结束调用')
            print('+'*60)
            
            # 将Java byte数组转换回Python bytes
            if result is not None:
                # 确保result是byte数组类型
                print(f'原始MAC结果：{result}')
                if isinstance(result, jpype.JArray(jpype.JByte)):
                    return True, bytes(result), None
                else:
                    return False, None, "返回结果类型不匹配"
            return False, None, "返回结果为null"
            
        #except jpype._jexception.cn.ccb.secapi.SecExceptionPyRaisable as e:
        #    error_msg = str(e)
        #    print(f"生成MAC错误: {error_msg}")
        #    return False, None, error_msg
        except Exception as e:
            print(f"生成MAC异常: {e}")
            return False, None, str(e)
    
    def get_error(self) -> Tuple[bool, Optional[Tuple[int, str]]]:
        """
        获取错误信息
        
        Returns:
            Tuple[bool, Optional[Tuple[int, str]]]: (是否成功, (错误码, 错误信息))
        """
        try:
            # 方法1: 尝试调用静态方法（如果存在）
            try:
                # 假设SecAPI有静态方法获取错误
                error_obj = self.SecAPI.getError()
                if error_obj:
                    # 假设错误对象有getCode()和getMessage()方法
                    if hasattr(error_obj, 'getCode') and hasattr(error_obj, 'getMessage'):
                        error_code = error_obj.getCode()
                        error_msg = error_obj.getMessage()
                        return True, (error_code, error_msg)
            except Exception:
                pass
            
            # 方法2: 尝试访问静态字段（如果存在）
            try:
                # 假设SecAPI有静态字段存储最后错误
                if hasattr(self.SecAPI, 'LAST_ERROR'):
                    error_obj = self.SecAPI.LAST_ERROR
                    if hasattr(error_obj, 'getCode') and hasattr(error_obj, 'getMessage'):
                        error_code = error_obj.getCode()
                        error_msg = error_obj.getMessage()
                        return True, (error_code, error_msg)
            except Exception:
                pass
            
            return False, None
            
        except Exception as e:
            print(f"获取错误信息异常: {e}")
            return False, None
    
    def shutdown(self):
        """
        关闭JVM
        """
        if jpype.isJVMStarted():
            jpype.shutdownJVM()
            print("JVM 已关闭")
    
    def __enter__(self):
        """
        进入上下文管理器时启动JVM
        """
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
