#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""汉语拼音转换工具."""

from __future__ import unicode_literals

__title__ = 'pypinyin'
__version__ = '0.5.5'
__author__ = 'mozillazg, 闲耘'
__license__ = 'MIT'
__copyright__ = 'Copyright (c) 2014 mozillazg, 闲耘'

__all__ = ['pinyin', 'lazy_pinyin', 'slug',
           'STYLE_NORMAL', 'NORMAL',
           'STYLE_TONE', 'TONE',
           'STYLE_TONE2', 'TONE2',
           'STYLE_INITIALS', 'INITIALS',
           'STYLE_FINALS', 'FINALS',
           'STYLE_FINALS_TONE', 'FINALS_TONE',
           'STYLE_FINALS_TONE2', 'FINALS_TONE2',
           'STYLE_FIRST_LETTER', 'FIRST_LETTER']

from copy import deepcopy
from itertools import chain
import re

from . import phrases_dict, phonetic_symbol, pinyin_dict

try:
    unicode = unicode   # python 2
except NameError:
    unicode = str       # python 3

# 词语拼音库
PHRASES_DICT = phrases_dict.phrases_dict.copy()
# 单字拼音库
PINYIN_DICT = pinyin_dict.pinyin_dict.copy()
# 声母表
_INITIALS = 'zh,ch,sh,b,p,m,f,d,t,n,l,g,k,h,j,q,x,r,z,c,s,yu,y,w'.split(',')
# 带声调字符与使用数字标识的字符的对应关系，类似： {u'ā': 'a1'}
PHONETIC_SYMBOL = phonetic_symbol.phonetic_symbol.copy()
# 所有的带声调字符
re_phonetic_symbol_source = ''.join(PHONETIC_SYMBOL.keys())
# 匹配带声调字符的正则表达式
RE_PHONETIC_SYMBOL = r'[' + re.escape(re_phonetic_symbol_source) + r']'
# 匹配使用数字标识声调的字符的正则表达式
RE_TONE2 = r'([aeoiuvnm])([0-4])$'

# 拼音风格
PINYIN_STYLE = {
    'NORMAL': 0,          # 普通风格，不带声调
    'TONE': 1,            # 标准风格，声调在韵母的第一个字母上
    'TONE2': 2,           # 声调在拼音之后，使用数字 1~4 标识
    'INITIALS': 3,        # 仅保留声母部分
    'FIRST_LETTER': 4,    # 仅保留首字母
    'FINALS': 5,          # 仅保留韵母部分，不带声调
    'FINALS_TONE': 6,     # 仅保留韵母部分，带声调
    'FINALS_TONE2': 7,    # 仅保留韵母部分，声调在拼音之后，使用数字 1~4 标识
}
# 普通风格，不带声调
NORMAL = STYLE_NORMAL = PINYIN_STYLE['NORMAL']
# 标准风格，声调在韵母的第一个字母上
TONE = STYLE_TONE = PINYIN_STYLE['TONE']
# 声调在拼音之后，使用数字 1~4 标识
TONE2 = STYLE_TONE2 = PINYIN_STYLE['TONE2']
# 仅保留声母部分
INITIALS = STYLE_INITIALS = PINYIN_STYLE['INITIALS']
# 仅保留首字母
FIRST_LETTER = STYLE_FIRST_LETTER = PINYIN_STYLE['FIRST_LETTER']
# 仅保留韵母部分，不带声调
FINALS = STYLE_FINALS = PINYIN_STYLE['FINALS']
# 仅保留韵母部分，带声调
FINALS_TONE = STYLE_FINALS_TONE = PINYIN_STYLE['FINALS_TONE']
# 仅保留韵母部分，声调在拼音之后，使用数字 1~4 标识
FINALS_TONE2 = STYLE_FINALS_TONE2 = PINYIN_STYLE['FINALS_TONE2']


def load_single_dict(pinyin_dict):
    """载入用户自定义的单字拼音库

    :param pinyin_dict: 单字拼音库。比如： ``{0x963F: u"ā,ē"}``
    :type pinyin_dict: dict
    """
    PINYIN_DICT.update(pinyin_dict)


def load_phrases_dict(phrases_dict):
    """载入用户自定义的词语拼音库

    :param phrases_dict: 词语拼音库。比如： ``{u"阿爸": [[u"ā"], [u"bà"]]}``
    :type phrases_dict: dict
    """
    PHRASES_DICT.update(phrases_dict)


def initial(pinyin):
    """获取单个拼音中的声母.

    :param pinyin: 单个拼音
    :type pinyin: unicode
    :return: 声母
    :rtype: unicode
    """
    for i in _INITIALS:
        if pinyin.startswith(i):
            return i
    return ''


def final(pinyin):
    """获取单个拼音中的韵母.

    :param pinyin: 单个拼音
    :type pinyin: unicode
    :return: 韵母
    :rtype: unicode
    """
    initial_ = initial(pinyin) or None
    if not initial_:
        return pinyin
    return ''.join(pinyin.split(initial_, 1))


def toFixed(pinyin, style):
    """根据拼音风格格式化带声调的拼音.

    :param pinyin: 单个拼音
    :param style: 拼音风格
    :return: 根据拼音风格格式化后的拼音字符串
    :rtype: unicode
    """
    # 声母
    if style == INITIALS:
        return initial(pinyin)

    def _replace(m):
        symbol = m.group(0)  # 带声调的字符
        # 不包含声调
        if style in [NORMAL, FIRST_LETTER, FINALS]:
            # 去掉声调: a1 -> a
            return re.sub(RE_TONE2, r'\1', PHONETIC_SYMBOL[symbol])
        # 使用数字标识声调
        elif style in [TONE2, FINALS_TONE2]:
            # 返回使用数字标识声调的字符
            return PHONETIC_SYMBOL[symbol]
        # 声调在头上
        else:
            return symbol

    # 替换拼音中的带声调字符
    py = re.sub(RE_PHONETIC_SYMBOL, _replace, pinyin)

    # 首字母
    if style == FIRST_LETTER:
        py = py[0]
    # 韵母
    elif style in [FINALS, FINALS_TONE, FINALS_TONE2]:
        py = final(py)
    return py


def _handle_nopinyin_char(char, errors='default'):
    """处理没有拼音的字符"""
    if errors == 'default':
        return char
    elif errors == 'ignore':
        return None
    elif errors == 'replace':
        return unicode('%x' % ord(char))


def single_pinyin(han, style, heteronym, errors='default'):
    """单字拼音转换.

    :param han: 单个汉字
    :param errors: 指定如何处理没有拼音的字符
    :return: 返回拼音列表，多音字会有多个拼音项
    :rtype: list
    """
    num = ord(han)
    if num not in PINYIN_DICT:
        py = _handle_nopinyin_char(han, errors=errors)
        return [py] if py else None
    pys = PINYIN_DICT[num].split(",")  # 字的拼音列表
    if not heteronym:
        return [toFixed(pys[0], style)]

    # 输出多音字的多个读音
    # 临时存储已存在的拼音，避免多音字拼音转换为非音标风格出现重复。
    py_cached = {}
    pinyins = []
    for i in pys:
        py = toFixed(i, style)
        if py in py_cached:
            continue
        py_cached[py] = py
        pinyins.append(py)
    return pinyins


def phrases_pinyin(phrases, style, heteronym, errors='default'):
    """词语拼音转换.

    :param phrases: 词语
    :param errors: 指定如何处理没有拼音的字符
    :return: 拼音列表
    :rtype: list
    """
    py = []
    if phrases in PHRASES_DICT:
        py = deepcopy(PHRASES_DICT[phrases])
        for idx, item in enumerate(py):
            py[idx] = [toFixed(item[0], style=style)]
    else:
        for i in phrases:
            single = single_pinyin(i, style=style, heteronym=heteronym,
                                   errors=errors)
            if single:
                py.append(single)
    return py


def _pinyin(words, style, heteronym, errors):
    re_hans = re.compile(r'''^(?:
                         [\u3400-\u4dbf]    # CJK 扩展 A:[3400-4DBF]
                         |[\u4e00-\u9fff]    # CJK 基本:[4E00-9FFF]
                         |[\uf900-\ufaff]    # CJK 兼容:[F900-FAFF]
                         )+$''', re.X)
    pys = []
    # 初步过滤没有拼音的字符
    if re_hans.match(words):
        pys = phrases_pinyin(words, style=style, heteronym=heteronym,
                             errors=errors)
    else:
        if re.match(r'^[a-zA-Z0-9_]+$', words):
            pys.append([words])
        else:
            for word in words:
                # 字母汉字混合的固定词组（这种情况来自分词结果）
                if not (re_hans.match(word)
                        or re.match(r'^[a-zA-Z0-9_]+$', word)
                        ):
                    py = _handle_nopinyin_char(word, errors=errors)
                    pys.append([py]) if py else None
                else:
                    pys.extend(_pinyin(word, style, heteronym, errors))
    return pys


def pinyin(hans, style=TONE, heteronym=False, errors='default'):
    """将汉字转换为拼音.

    :param hans: 汉字字符串( ``u'你好吗'`` )或列表( ``[u'你好', u'吗']`` ).

                 如果用户安装了 ``jieba`` , 将使用 ``jieba`` 对字符串进行
                 分词处理。

                 也可以使用自己喜爱的分词模块对字符串进行分词处理,
                 只需将经过分词处理的字符串列表传进来就可以了。
    :type hans: unicode 字符串或字符串列表
    :param style: 指定拼音风格
    :param errors: 指定如何处理没有拼音的字符

                   * ``'default'``: 保留原始字符
                   * ``'ignore'``: 忽略该字符
                   * ``'replace'``: 替换为去掉 ``\\u`` 的 unicode 编码字符串
                     (``u'\\u90aa'`` => ``u'90aa'``)
    :param heteronym: 是否启用多音字
    :return: 拼音列表
    :rtype: list

    Usage::

      >>> from pypinyin import pinyin
      >>> import pypinyin
      >>> pinyin(u'中心')
      [[u'zh\u014dng'], [u'x\u012bn']]
      >>> pinyin(u'中心', heteronym=True)  # 启用多音字模式
      [[u'zh\u014dng', u'zh\xf2ng'], [u'x\u012bn']]
      >>> pinyin(u'中心', style=pypinyin.INITIALS)  # 设置拼音风格
      [[u'zh'], [u'x']]
      >>> pinyin(u'中心', style=pypinyin.TONE2)
      [[u'zho1ng'], [u'xi1n']]
    """
    # 对字符串进行分词处理
    if isinstance(hans, unicode):
        try:
            import jieba
            hans = jieba.cut(hans)
        except ImportError:
            pass
    pys = []
    for words in hans:
        pys.extend(_pinyin(words, style, heteronym, errors))
    return pys


def slug(hans, style=NORMAL, heteronym=False, separator='-', errors='default'):
    """生成 slug 字符串.

    :param hans: 汉字
    :type hans: unicode or list
    :param style: 指定拼音风格
    :param heteronym: 是否启用多音字
    :param separstor: 两个拼音间的分隔符/连接符
    :param errors: 指定如何处理没有拼音的字符
    :return: slug 字符串.

    ::

      >>> import pypinyin
      >>> pypinyin.slug(u'中国人')
      u'zhong-guo-ren'
      >>> pypinyin.slug(u'中国人', separator=u' ')
      u'zhong guo ren'
      >>> pypinyin.slug(u'中国人', style=pypinyin.INITIALS)
      u'zh-g-r'
    """
    return separator.join(chain(*pinyin(hans, style=style, heteronym=heteronym,
                                        errors=errors)
                                ))


def lazy_pinyin(hans, style=NORMAL, errors='default'):
    """不包含多音字的拼音列表.

    与 :py:func:`~pypinyin.pinyin` 的区别是返回的拼音是个字符串，
    并且每个字只包含一个读音.

    :param hans: 汉字
    :type hans: unicode or list
    :param style: 指定拼音风格
    :param errors: 指定如何处理没有拼音的字符
    :return: 拼音列表(e.g. ``['zhong', 'guo', 'ren']``)
    :rtype: list

    Usage::

      >>> from pypinyin import lazy_pinyin
      >>> import pypinyin
      >>> lazy_pinyin(u'中心')
      [u'zhong', u'xin']
      >>> lazy_pinyin(u'中心', style=pypinyin.TONE)
      [u'zh\u014dng', u'x\u012bn']
      >>> lazy_pinyin(u'中心', style=pypinyin.INITIALS)
      [u'zh', u'x']
      >>> lazy_pinyin(u'中心', style=pypinyin.TONE2)
      [u'zho1ng', u'xi1n']
    """
    return list(chain(*pinyin(hans, style=style, heteronym=False,
                              errors=errors)))
