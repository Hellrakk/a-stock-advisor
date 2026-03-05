#!/usr/bin/env python3
"""
系统管理器
功能：
1. 统一管理系统组件
2. 提供插件系统
3. 配置管理
4. 系统健康检查
5. 组件间通信
"""

import importlib
import os
import sys
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Plugin:
    """插件基类"""
    
    def __init__(self, name: str, config: Dict = None):
        self.name = name
        self.config = config or {}
        self.enabled = True
    
    def initialize(self) -> bool:
        """初始化插件"""
        raise NotImplementedError
    
    def run(self, *args, **kwargs) -> Any:
        """运行插件"""
        raise NotImplementedError
    
    def shutdown(self) -> bool:
        """关闭插件"""
        raise NotImplementedError


class PluginManager:
    """插件管理器"""
    
    def __init__(self):
        self.plugins = {}
        self.plugin_paths = []
    
    def add_plugin_path(self, path: str):
        """添加插件路径"""
        if os.path.exists(path):
            self.plugin_paths.append(path)
            sys.path.insert(0, path)
            logger.info(f"添加插件路径: {path}")
    
    def load_plugin(self, name: str, module_path: str, config: Dict = None) -> bool:
        """加载插件"""
        try:
            # 动态导入模块
            module = importlib.import_module(module_path)
            # 假设插件类名为Plugin
            plugin_class = getattr(module, 'Plugin', None)
            if not plugin_class:
                logger.error(f"插件 {name} 中未找到Plugin类")
                return False
            
            # 创建插件实例
            plugin = plugin_class(name, config)
            if plugin.initialize():
                self.plugins[name] = plugin
                logger.info(f"插件 {name} 加载成功")
                return True
            else:
                logger.error(f"插件 {name} 初始化失败")
                return False
        except Exception as e:
            logger.error(f"加载插件 {name} 失败: {e}")
            return False
    
    def run_plugin(self, name: str, *args, **kwargs) -> Any:
        """运行插件"""
        if name in self.plugins and self.plugins[name].enabled:
            try:
                return self.plugins[name].run(*args, **kwargs)
            except Exception as e:
                logger.error(f"运行插件 {name} 失败: {e}")
                return None
        else:
            logger.warning(f"插件 {name} 未加载或未启用")
            return None
    
    def shutdown_plugin(self, name: str) -> bool:
        """关闭插件"""
        if name in self.plugins:
            try:
                result = self.plugins[name].shutdown()
                del self.plugins[name]
                logger.info(f"插件 {name} 关闭成功")
                return result
            except Exception as e:
                logger.error(f"关闭插件 {name} 失败: {e}")
                return False
        return False
    
    def shutdown_all(self):
        """关闭所有插件"""
        for name in list(self.plugins.keys()):
            self.shutdown_plugin(name)


class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_dir: str = 'config'):
        self.config_dir = config_dir
        self.configs = {}
        self._load_configs()
    
    def _load_configs(self):
        """加载配置文件"""
        if not os.path.exists(self.config_dir):
            os.makedirs(self.config_dir)
            logger.info(f"创建配置目录: {self.config_dir}")
        
        for filename in os.listdir(self.config_dir):
            if filename.endswith('.json'):
                filepath = os.path.join(self.config_dir, filename)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        config = json.load(f)
                        config_name = filename.replace('.json', '')
                        self.configs[config_name] = config
                        logger.info(f"加载配置: {config_name}")
                except Exception as e:
                    logger.error(f"加载配置文件 {filename} 失败: {e}")
    
    def get(self, config_name: str, key: str = None, default: Any = None) -> Any:
        """获取配置"""
        if config_name not in self.configs:
            return default
        
        if key is None:
            return self.configs[config_name]
        
        return self.configs[config_name].get(key, default)
    
    def set(self, config_name: str, key: str, value: Any):
        """设置配置"""
        if config_name not in self.configs:
            self.configs[config_name] = {}
        
        self.configs[config_name][key] = value
        
        # 保存配置
        filepath = os.path.join(self.config_dir, f"{config_name}.json")
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.configs[config_name], f, indent=2, ensure_ascii=False)
            logger.info(f"保存配置: {config_name}")
        except Exception as e:
            logger.error(f"保存配置文件 {config_name} 失败: {e}")
    
    def reload(self):
        """重新加载配置"""
        self._load_configs()
        logger.info("配置重新加载完成")


class SystemManager:
    """系统管理器"""
    
    def __init__(self, base_dir: str = None):
        self.base_dir = base_dir or os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.config_manager = ConfigManager(os.path.join(self.base_dir, 'config'))
        self.plugin_manager = PluginManager()
        self.components = {}
        self.system_status = 'initialized'
        self.start_time = datetime.now()
        
        # 添加插件路径
        plugin_dir = os.path.join(self.base_dir, 'plugins')
        if os.path.exists(plugin_dir):
            self.plugin_manager.add_plugin_path(plugin_dir)
    
    def register_component(self, name: str, component: Any):
        """注册组件"""
        self.components[name] = component
        logger.info(f"组件 {name} 注册成功")
    
    def get_component(self, name: str) -> Optional[Any]:
        """获取组件"""
        return self.components.get(name)
    
    def load_plugin(self, name: str, module_path: str, config: Dict = None) -> bool:
        """加载插件"""
        return self.plugin_manager.load_plugin(name, module_path, config)
    
    def run_plugin(self, name: str, *args, **kwargs) -> Any:
        """运行插件"""
        return self.plugin_manager.run_plugin(name, *args, **kwargs)
    
    def get_config(self, config_name: str, key: str = None, default: Any = None) -> Any:
        """获取配置"""
        return self.config_manager.get(config_name, key, default)
    
    def set_config(self, config_name: str, key: str, value: Any):
        """设置配置"""
        self.config_manager.set(config_name, key, value)
    
    def health_check(self) -> Dict:
        """系统健康检查"""
        check_time = datetime.now()
        uptime = (check_time - self.start_time).total_seconds()
        
        # 检查组件状态
        component_status = {}
        for name, component in self.components.items():
            try:
                # 检查组件是否有health_check方法
                if hasattr(component, 'health_check'):
                    status = component.health_check()
                else:
                    status = {'status': 'ok'}
                component_status[name] = status
            except Exception as e:
                component_status[name] = {'status': 'error', 'message': str(e)}
        
        # 检查插件状态
        plugin_status = {}
        for name, plugin in self.plugin_manager.plugins.items():
            plugin_status[name] = {'enabled': plugin.enabled}
        
        health_report = {
            'timestamp': check_time.strftime('%Y-%m-%d %H:%M:%S'),
            'system_status': self.system_status,
            'uptime_seconds': uptime,
            'components': component_status,
            'plugins': plugin_status,
            'configs_loaded': list(self.config_manager.configs.keys())
        }
        
        return health_report
    
    def start(self):
        """启动系统"""
        logger.info("🚀 启动系统...")
        
        # 初始化组件
        for name, component in self.components.items():
            if hasattr(component, 'initialize'):
                try:
                    component.initialize()
                    logger.info(f"组件 {name} 初始化成功")
                except Exception as e:
                    logger.error(f"组件 {name} 初始化失败: {e}")
        
        self.system_status = 'running'
        logger.info("✅ 系统启动完成")
    
    def shutdown(self):
        """关闭系统"""
        logger.info("🛑 关闭系统...")
        
        # 关闭插件
        self.plugin_manager.shutdown_all()
        
        # 关闭组件
        for name, component in self.components.items():
            if hasattr(component, 'shutdown'):
                try:
                    component.shutdown()
                    logger.info(f"组件 {name} 关闭成功")
                except Exception as e:
                    logger.error(f"组件 {name} 关闭失败: {e}")
        
        self.system_status = 'shutdown'
        logger.info("✅ 系统关闭完成")
    
    def generate_system_report(self) -> Dict:
        """生成系统报告"""
        report = {
            'system': {
                'base_dir': self.base_dir,
                'status': self.system_status,
                'start_time': self.start_time.strftime('%Y-%m-%d %H:%M:%S'),
                'uptime': (datetime.now() - self.start_time).total_seconds()
            },
            'components': list(self.components.keys()),
            'plugins': list(self.plugin_manager.plugins.keys()),
            'configs': list(self.config_manager.configs.keys()),
            'health': self.health_check()
        }
        
        return report
    
    def save_system_report(self, output_dir: str = 'reports/system'):
        """保存系统报告"""
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report = self.generate_system_report()
        
        # 保存JSON报告
        json_path = os.path.join(output_dir, f'system_report_{timestamp}.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        
        # 生成Markdown报告
        md_report = self._generate_markdown_report(report)
        md_path = os.path.join(output_dir, f'system_report_{timestamp}.md')
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(md_report)
        
        logger.info(f"系统报告已保存: {json_path}")
        logger.info(f"Markdown报告已保存: {md_path}")
        
        return json_path
    
    def _generate_markdown_report(self, report: Dict) -> str:
        """生成Markdown报告"""
        lines = []
        
        lines.append("# 系统状态报告\n")
        lines.append(f"**生成时间:** {report['health']['timestamp']}\n")
        lines.append("---\n")
        
        # 系统信息
        lines.append("## 🖥️ 系统信息\n")
        lines.append(f"- **基础目录:** {report['system']['base_dir']}\n")
        lines.append(f"- **系统状态:** {report['system']['status']}\n")
        lines.append(f"- **启动时间:** {report['system']['start_time']}\n")
        lines.append(f"- **运行时间:** {report['system']['uptime']:.2f} 秒\n\n")
        
        # 组件状态
        lines.append("## 🧩 组件状态\n")
        if report['components']:
            for component in report['components']:
                status = report['health']['components'].get(component, {'status': 'unknown'})
                status_str = status.get('status', 'unknown')
                if status_str == 'ok':
                    status_str = '✅ 正常'
                elif status_str == 'error':
                    status_str = f"❌ 错误: {status.get('message', '未知错误')}"
                elif status_str == 'disabled':
                    status_str = '⏸️ 禁用'
                lines.append(f"- **{component}:** {status_str}\n")
        else:
            lines.append("无组件\n")
        lines.append("\n")
        
        # 插件状态
        lines.append("## 🔌 插件状态\n")
        if report['plugins']:
            for plugin in report['plugins']:
                enabled = report['health']['plugins'].get(plugin, {'enabled': False})['enabled']
                status_str = "✅ 启用" if enabled else "⏸️ 禁用"
                lines.append(f"- **{plugin}:** {status_str}\n")
        else:
            lines.append("无插件\n")
        lines.append("\n")
        
        # 配置信息
        lines.append("## ⚙️ 配置信息\n")
        if report['configs']:
            lines.append("已加载的配置文件:\n")
            for config in report['configs']:
                lines.append(f"- {config}.json\n")
        else:
            lines.append("无配置文件\n")
        lines.append("\n")
        
        lines.append("---\n")
        lines.append("\n## 📋 总结\n\n")
        lines.append("本报告提供了系统的详细状态信息，包括组件、插件和配置的运行情况。\n")
        
        return '\n'.join(lines)


class DataPipeline:
    """数据处理管道"""
    
    def __init__(self, system_manager: SystemManager):
        self.system_manager = system_manager
        self.steps = []
    
    def add_step(self, name: str, func: callable, **kwargs):
        """添加处理步骤"""
        self.steps.append({'name': name, 'func': func, 'kwargs': kwargs})
        logger.info(f"添加数据处理步骤: {name}")
    
    def run(self, data: Any) -> Any:
        """运行数据处理管道"""
        logger.info("运行数据处理管道...")
        
        current_data = data
        for step in self.steps:
            try:
                logger.info(f"执行步骤: {step['name']}")
                current_data = step['func'](current_data, **step['kwargs'])
            except Exception as e:
                logger.error(f"执行步骤 {step['name']} 失败: {e}")
                return None
        
        logger.info("数据处理管道执行完成")
        return current_data
    
    def clear(self):
        """清空处理步骤"""
        self.steps = []
        logger.info("数据处理管道已清空")


if __name__ == '__main__':
    # 测试系统管理器
    print("=== 系统管理器测试 ===")
    
    # 创建系统管理器
    system_manager = SystemManager()
    
    # 注册组件
    class TestComponent:
        def initialize(self):
            logger.info("测试组件初始化")
        
        def health_check(self):
            return {'status': 'ok'}
    
    system_manager.register_component('test_component', TestComponent())
    
    # 启动系统
    system_manager.start()
    
    # 运行健康检查
    health = system_manager.health_check()
    print(f"系统健康状态: {health['system_status']}")
    
    # 生成系统报告
    system_manager.save_system_report()
    
    # 关闭系统
    system_manager.shutdown()
