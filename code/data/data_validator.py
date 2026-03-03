#!/usr/bin/env python3
"""
数据验证模块
验证市场数据、个股数据的合理性，防止异常数据
"""

import re
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataValidator:
    """数据验证器"""
    
    INDEX_RANGES = {
        'sh': {'min': 2000, 'max': 8000, 'name': '上证指数'},
        'sz': {'min': 5000, 'max': 20000, 'name': '深证成指'},
        'cyb': {'min': 1500, 'max': 5000, 'name': '创业板指'},
        'hs300': {'min': 2500, 'max': 8000, 'name': '沪深300'}
    }
    
    MARKET_VOLUME_RANGE = {'min': 5000, 'max': 50000}
    
    STOCK_PRICE_RANGE = {'min': 0.1, 'max': 2000}
    
    PE_RANGE = {'min': -500, 'max': 5000}
    PB_RANGE = {'min': -10, 'max': 100}
    ROE_RANGE = {'min': -100, 'max': 200}
    MARKET_CAP_RANGE = {'min': 5, 'max': 50000}
    
    VOLATILITY_RANGE = {'min': 5, 'max': 150}
    VAR_RANGE = {'min': 0, 'max': 30}
    DRAWDOWN_RANGE = {'min': 0, 'max': 100}
    
    def __init__(self, strict: bool = True):
        """
        初始化
        
        Args:
            strict: 是否严格模式（严格模式下警告也会报错）
        """
        self.strict = strict
        self.errors = []
        self.warnings = []
    
    def validate_index_data(self, index_data: Dict) -> Tuple[bool, List[str]]:
        """
        验证指数数据
        
        Args:
            index_data: 指数数据字典
            
        Returns:
            (是否有效, 错误信息列表)
        """
        errors = []
        warnings = []
        
        for key, config in self.INDEX_RANGES.items():
            if key not in index_data:
                errors.append(f"缺少{config['name']}数据")
                continue
            
            data = index_data[key]
            price = data.get('price', 0)
            change_pct = data.get('change_pct', 0)
            
            if price == 0:
                errors.append(f"{config['name']}价格为0")
            elif not (config['min'] <= price <= config['max']):
                errors.append(
                    f"{config['name']}价格{price}超出合理范围[{config['min']}, {config['max']}]"
                )
            
            if abs(change_pct) > 20:
                warnings.append(
                    f"{config['name']}单日涨跌幅{change_pct}%异常"
                )
        
        self.errors.extend(errors)
        self.warnings.extend(warnings)
        
        is_valid = len(errors) == 0 and (not self.strict or len(warnings) == 0)
        return is_valid, errors + warnings
    
    def validate_market_sentiment(self, sentiment: Dict) -> Tuple[bool, List[str]]:
        """
        验证市场情绪数据
        
        Args:
            sentiment: 市场情绪数据
            
        Returns:
            (是否有效, 错误信息列表)
        """
        errors = []
        warnings = []
        
        up_count = sentiment.get('up_count', 0)
        down_count = sentiment.get('down_count', 0)
        total_volume = sentiment.get('total_volume', 0)
        north_flow = sentiment.get('north_flow', 0)
        
        if up_count < 0 or up_count > 5000:
            errors.append(f"涨停数量{up_count}不合理")
        
        if down_count < 0 or down_count > 5000:
            errors.append(f"跌停数量{down_count}不合理")
        
        if total_volume > 0:
            if not (self.MARKET_VOLUME_RANGE['min'] <= total_volume <= self.MARKET_VOLUME_RANGE['max']):
                errors.append(
                    f"成交量{total_volume}亿超出合理范围"
                )
        
        if abs(north_flow) > 500:
            warnings.append(f"北向资金{north_flow}亿数值较大，请核实")
        
        self.errors.extend(errors)
        self.warnings.extend(warnings)
        
        is_valid = len(errors) == 0 and (not self.strict or len(warnings) == 0)
        return is_valid, errors + warnings
    
    def validate_stock_data(self, stock_data: Dict) -> Tuple[bool, List[str]]:
        """
        验证个股数据
        
        Args:
            stock_data: 个股数据
            
        Returns:
            (是否有效, 错误信息列表)
        """
        errors = []
        warnings = []
        
        code = stock_data.get('code', '')
        name = stock_data.get('name', '')
        
        if not code:
            errors.append("股票代码为空")
        
        price = stock_data.get('price', 0)
        if price > 0:
            if not (self.STOCK_PRICE_RANGE['min'] <= price <= self.STOCK_PRICE_RANGE['max']):
                errors.append(f"{name}({code})价格{price}超出合理范围")
        else:
            errors.append(f"{name}({code})价格无效: {price}")
        
        change_pct = stock_data.get('change_pct', 0)
        if abs(change_pct) > 20:
            warnings.append(f"{name}({code})涨跌幅{change_pct}%异常")
        
        pe = stock_data.get('pe_ttm')
        if pe is not None:
            if not (self.PE_RANGE['min'] <= pe <= self.PE_RANGE['max']):
                warnings.append(f"{name}({code})PE({pe})超出常规范围")
        
        pb = stock_data.get('pb')
        if pb is not None:
            if not (self.PB_RANGE['min'] <= pb <= self.PB_RANGE['max']):
                warnings.append(f"{name}({code})PB({pb})超出常规范围")
        
        roe = stock_data.get('roe')
        if roe is not None:
            if not (self.ROE_RANGE['min'] <= roe <= self.ROE_RANGE['max']):
                warnings.append(f"{name}({code})ROE({roe}%)超出常规范围")
        
        market_cap = stock_data.get('market_cap')
        if market_cap is not None and market_cap > 0:
            if not (self.MARKET_CAP_RANGE['min'] <= market_cap <= self.MARKET_CAP_RANGE['max']):
                errors.append(
                    f"{name}({code})市值{market_cap}亿超出合理范围"
                )
        
        self.errors.extend(errors)
        self.warnings.extend(warnings)
        
        is_valid = len(errors) == 0 and (not self.strict or len(warnings) == 0)
        return is_valid, errors + warnings
    
    def validate_risk_metrics(self, risk_data: Dict) -> Tuple[bool, List[str]]:
        """
        验证风险指标
        
        Args:
            risk_data: 风险指标数据
            
        Returns:
            (是否有效, 错误信息列表)
        """
        errors = []
        warnings = []
        
        volatility = risk_data.get('volatility', 0)
        if volatility > 0:
            if not (self.VOLATILITY_RANGE['min'] <= volatility <= self.VOLATILITY_RANGE['max']):
                errors.append(f"波动率{volatility}%超出合理范围")
        
        var_95 = risk_data.get('var_95', 0)
        if var_95 > 0:
            if not (self.VAR_RANGE['min'] <= var_95 <= self.VAR_RANGE['max']):
                warnings.append(f"VaR(95%){var_95}%超出常规范围")
        
        max_dd = risk_data.get('max_drawdown', 0)
        if max_dd > 0:
            if not (self.DRAWDOWN_RANGE['min'] <= max_dd <= self.DRAWDOWN_RANGE['max']):
                errors.append(f"最大回撤{max_dd}%超出合理范围")
        
        beta = risk_data.get('beta')
        if beta is not None:
            if not (-2 <= beta <= 3):
                warnings.append(f"Beta值{beta}超出常规范围[-2, 3]")
        
        sharpe = risk_data.get('sharpe_ratio')
        if sharpe is not None:
            if sharpe < -5 or sharpe > 10:
                warnings.append(f"夏普比率{sharpe}超出常规范围")
        
        self.errors.extend(errors)
        self.warnings.extend(warnings)
        
        is_valid = len(errors) == 0 and (not self.strict or len(warnings) == 0)
        return is_valid, errors + warnings
    
    def validate_industry_weights(self, weights: Dict[str, float]) -> Tuple[bool, List[str]]:
        """
        验证行业权重
        
        Args:
            weights: 行业权重字典
            
        Returns:
            (是否有效, 错误信息列表)
        """
        errors = []
        warnings = []
        
        if not weights:
            errors.append("行业权重数据为空")
            return False, errors
        
        total_weight = sum(weights.values())
        
        if abs(total_weight - 100) > 5:
            errors.append(f"行业权重总和{total_weight:.2f}%不等于100%")
        
        for industry, weight in weights.items():
            if weight < 0:
                errors.append(f"行业{industry}权重为负: {weight}%")
            elif weight > 60:
                warnings.append(f"行业{industry}权重过高: {weight}%")
        
        self.errors.extend(errors)
        self.warnings.extend(warnings)
        
        is_valid = len(errors) == 0 and (not self.strict or len(warnings) == 0)
        return is_valid, errors + warnings
    
    def validate_all(self, data: Dict) -> Tuple[bool, Dict[str, List[str]]]:
        """
        验证所有数据
        
        Args:
            data: 包含所有数据的字典
            
        Returns:
            (是否全部有效, {数据类型: 错误信息列表})
        """
        all_errors = {}
        
        if 'index' in data:
            _, errors = self.validate_index_data(data['index'])
            if errors:
                all_errors['index'] = errors
        
        if 'sentiment' in data:
            _, errors = self.validate_market_sentiment(data['sentiment'])
            if errors:
                all_errors['sentiment'] = errors
        
        if 'stocks' in data:
            stock_errors = []
            for stock in data['stocks']:
                _, errors = self.validate_stock_data(stock)
                stock_errors.extend(errors)
            if stock_errors:
                all_errors['stocks'] = stock_errors
        
        if 'risk' in data:
            _, errors = self.validate_risk_metrics(data['risk'])
            if errors:
                all_errors['risk'] = errors
        
        if 'industry_weights' in data:
            _, errors = self.validate_industry_weights(data['industry_weights'])
            if errors:
                all_errors['industry_weights'] = errors
        
        is_valid = len(all_errors) == 0
        return is_valid, all_errors
    
    def get_validation_report(self) -> str:
        """获取验证报告"""
        lines = ["=" * 50, "数据验证报告", "=" * 50]
        
        if self.errors:
            lines.append("\n【错误】")
            for i, err in enumerate(self.errors, 1):
                lines.append(f"  {i}. {err}")
        
        if self.warnings:
            lines.append("\n【警告】")
            for i, warn in enumerate(self.warnings, 1):
                lines.append(f"  {i}. {warn}")
        
        if not self.errors and not self.warnings:
            lines.append("\n✓ 所有数据验证通过")
        
        return "\n".join(lines)
    
    def clear(self):
        """清空错误和警告"""
        self.errors = []
        self.warnings = []


class StockDataSanitizer:
    """股票数据清洗器"""
    
    @staticmethod
    def sanitize_pe(pe: Optional[float]) -> Optional[float]:
        """清洗PE数据"""
        if pe is None:
            return None
        if pe < -500 or pe > 5000:
            return None
        return round(pe, 2)
    
    @staticmethod
    def sanitize_pb(pb: Optional[float]) -> Optional[float]:
        """清洗PB数据"""
        if pb is None:
            return None
        if pb < -10 or pb > 100:
            return None
        return round(pb, 2)
    
    @staticmethod
    def sanitize_roe(roe: Optional[float]) -> Optional[float]:
        """清洗ROE数据"""
        if roe is None:
            return None
        if roe < -100 or roe > 200:
            return None
        return round(roe, 2)
    
    @staticmethod
    def sanitize_market_cap(market_cap: Optional[float]) -> Optional[float]:
        """清洗市值数据"""
        if market_cap is None:
            return None
        if market_cap < 0:
            return None
        return round(market_cap, 2)
    
    @staticmethod
    def sanitize_stock_data(data: Dict) -> Dict:
        """清洗股票数据"""
        sanitized = data.copy()
        
        sanitized['pe_ttm'] = StockDataSanitizer.sanitize_pe(data.get('pe_ttm'))
        sanitized['pb'] = StockDataSanitizer.sanitize_pb(data.get('pb'))
        sanitized['roe'] = StockDataSanitizer.sanitize_roe(data.get('roe'))
        sanitized['roa'] = StockDataSanitizer.sanitize_roe(data.get('roa'))
        sanitized['market_cap'] = StockDataSanitizer.sanitize_market_cap(data.get('market_cap'))
        
        if 'price' in data and data['price'] > 0:
            sanitized['price'] = round(data['price'], 2)
        
        if 'change_pct' in data:
            sanitized['change_pct'] = round(data['change_pct'], 2)
        
        return sanitized


if __name__ == "__main__":
    validator = DataValidator(strict=False)
    sanitizer = StockDataSanitizer()
    
    print("=" * 60)
    print("数据验证模块测试")
    print("=" * 60)
    
    test_index = {
        'sh': {'name': '上证指数', 'price': 4182.59, 'change_pct': 0.47},
        'sz': {'name': '深证成指', 'price': 14465.79, 'change_pct': -0.20},
        'cyb': {'name': '创业板指', 'price': 3294.16, 'change_pct': -0.49},
        'hs300': {'name': '沪深300', 'price': 3950.23, 'change_pct': 0.15}
    }
    
    print("\n【指数数据验证】")
    is_valid, errors = validator.validate_index_data(test_index)
    print(f"  有效: {is_valid}")
    if errors:
        print(f"  问题: {errors}")
    
    test_stock = {
        'code': 'sh600000',
        'name': '浦发银行',
        'price': 9.68,
        'change_pct': -0.41,
        'pe_ttm': 5.23,
        'pb': 0.45,
        'roe': 10.5,
        'market_cap': 3224.5
    }
    
    print("\n【个股数据验证】")
    is_valid, errors = validator.validate_stock_data(test_stock)
    print(f"  有效: {is_valid}")
    if errors:
        print(f"  问题: {errors}")
    
    print("\n【验证报告】")
    print(validator.get_validation_report())
