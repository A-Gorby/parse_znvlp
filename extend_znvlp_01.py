import pandas as pd
import numpy as np
import os, sys, glob
import humanize
import re
import xlrd

import json
import itertools
#from urllib.request import urlopen
#import requests, xmltodict
import time, datetime
import math
from pprint import pprint
import gc
from tqdm import tqdm
tqdm.pandas()
import pickle

import logging
import zipfile
import warnings
import argparse

import numba
import numexpr as ne
# numba.set_num_threads(numba.get_num_threads())

# import g
from utils_znvlp import unzip_file, find_last_fn_pickle, restore_df_from_pickle, find_last_file
from utils_znvlp import save_df_to_pickle, get_humanize_filesize, exract_esklp_date, save_df_to_excel
# from extend_functions import *
# from xml_utils import load_smnn, create_smnn_list_df, reformat_smnn_list_df
# from xml_utils import load_klp_list, create_klp_list_dict_df, reformat_klp_list_dict_df

# logger = logging.getLogger('Extend ZNVLP')

logging.basicConfig(
    #level=logging.DEBUG # allow DEBUG level messages to pass through the logger
    # format='%(asctime)s - %(message)s', datefmt='%d-%m-%Y %H:%M:%S',
    format='%(asctime)s - %(message)s', datefmt='%H:%M:%S',
    level=logging.INFO # allow INFO level messages to pass through the logger
    # filename="py_log.log",filemode="w"
    )

from utils_io_znvlp import logger
if len(logger.handlers) > 1:
    for handler in logger.handlers:
        logger.removeHandler(handler)
    from utils_io_znvlp import logger
    # del logger
    # logger = Logger().logger
    # logger.propagate = False

# fn_esklp_xml_active_zip = 'esklp_20221110_active_21.5_00001.xml.zip'
# path_znvlp_source = 'D:/DPP/01_parsing/data/znvlp/source/'
# path_znvlp_work = 'D:/DPP/01_parsing/data/znvlp/temp/'
# path_znvlp_processed = 'D:/DPP/01_parsing/data/znvlp/processed/'
# path_esklp_processed = 'D:/DPP/01_parsing/data/esklp/processed/'
smnn_prefix = 'smnn_list_df_esklp'
klp_prefix = 'klp_list_dict_df_esklp'
# пока только по актиному ЕСКЛП
smnn_prefix = 'smnn_list_df_esklp_active'
klp_prefix = 'klp_list_dict_df_esklp_active'
xlsx_suffix = '.xlsx'
pickle_suffix = '.pickle'
# fn_smnn_list_df_pickle
#!unzip
doze_vol_handler_types = [ [0, True, False, False, False],
                          [1, True, True, True, True],
                          [2, True, False, True, False],
                          [3, False, False, True, False],
                          [4, False, False, True, False],
                          [5, True, True, True, True],
                          [6, True, False, False, False],
                          [7, True, True, True, True],
                          [8, True, True, True, False], 
                          [9, True, True, False, False],
                          [10, True, True, False, False], #соеденить с группой №9
                          [11, False, False, True, False],
                          [-1, True, True, True, False],
]
#doze_units_groups, vol_units_groups, doze_vol_handler_types
doze_vol_pharm_form_handlers = {
  'Таблетки':         doze_vol_handler_types[0],
  'Капсулы':          doze_vol_handler_types[0],
  'Драже':            doze_vol_handler_types[0],
  'Суппозитории':     doze_vol_handler_types[0],
  'Пастилки':         doze_vol_handler_types[0],
  'Имплантат':        doze_vol_handler_types[0],
  'Крем':             doze_vol_handler_types[1],
  'Мазь':             doze_vol_handler_types[1],
  'Гель':             doze_vol_handler_types[1],
  'Линимент':         doze_vol_handler_types[1],
  'Паста':            doze_vol_handler_types[1],
  # 'Газ медицинский':  doze_vol_handler_types[2],
  'Газ':              doze_vol_handler_types[2],
  'Клей':             doze_vol_handler_types[3],
  'Масло':            doze_vol_handler_types[4],
  'Настойка':         doze_vol_handler_types[4],
  'Жидкость':         doze_vol_handler_types[4],
  'Капли':            doze_vol_handler_types[5],
  'Концентрат':       doze_vol_handler_types[5],
  'Раствор':          doze_vol_handler_types[5],
  'Растворитель':     doze_vol_handler_types[5],
  'Сироп':            doze_vol_handler_types[5],
  'Суспензия':        doze_vol_handler_types[5],
  'Эмульсия':         doze_vol_handler_types[5],
  'Лиофилизат':       doze_vol_handler_types[6],
  'Порошок':          doze_vol_handler_types[6],
  'Аэрозоль':         doze_vol_handler_types[7],
  'Спрей':            doze_vol_handler_types[7],
  'Гранулы':          doze_vol_handler_types[8],
  'Микросферы':       doze_vol_handler_types[8],
  'Губка':            doze_vol_handler_types[9],
  'Пластырь':         doze_vol_handler_types[9],
  'Система':          doze_vol_handler_types[10],
  'Напиток':          doze_vol_handler_types[11],
  'Питание':          doze_vol_handler_types[11],
  'Смесь':            doze_vol_handler_types[11],
  'ph_f_undefined':   doze_vol_handler_types[-1]
}

smnn_list_df, klp_list_dict_df, zvnlp_df = None, None, None
np_lim_price_barcode_str = None 
np_lim_price_reg_date_str =  None 
znvlp_date = None
znvlp_date_format = None
esklp_date = None

def np_unique_nan(lst: np.array, debug = False)->np.array: # a la version 2.4
    lst_unique = None
    if lst is None or (((type(lst)==float) or (type(lst)==np.float64)) and np.isnan(lst)):
        # if debug: print('np_unique_nan:','lst is None or (((type(lst)==float) or (type(lst)==np.float64)) and math.isnan(lst))')
        lst_unique = lst
    else:
        data_types_set = list(set([type(i) for i in lst]))
        if debug: print('np_unique_nan:', 'lst:', lst, 'data_types_set:', data_types_set)
        if ((type(lst)==list) or (type(lst)==np.ndarray)):
            if debug: print('np_unique_nan:','if ((type(lst)==list) or (type(lst)==np.ndarray)):')
            if len(data_types_set) > 1: # несколько типов данных
                if list not in data_types_set and dict not in data_types_set and tuple not in data_types_set and type(None) not in data_types_set:
                    lst_unique = np.array(list(set(lst)), dtype=object)
                else:
                    lst_unique = lst
            elif len(data_types_set) == 1:
                if debug: print("np_unique_nan: elif len(data_types_set) == 1:")
                if list in data_types_set:
                    lst_unique = np.unique(np.array(lst, dtype=object))
                elif  np.ndarray in data_types_set:
                    # print('elif  np.ndarray in data_types_set :')
                    lst_unique = np.unique(lst.astype(object))
                    # lst_unique = np_unique_nan(lst_unique)
                    lst_unique = np.asarray(lst, dtype = object)
                    # lst_unique = np.unique(lst_unique)
                elif type(None) in data_types_set:
                    # lst_unique = np.array(list(set(lst)))
                    lst_unique = np.array(list(set(list(lst))))
                elif dict in  data_types_set:
                    lst_unique = lst
                    # np.unique(lst)
                elif type(lst) == np.ndarray:
                    if debug: print("np_unique_nan: type(lst) == np.ndarray")
                    if (lst.dtype.kind == 'f') or  (lst.dtype == np.float64) or  (float in data_types_set):
                        if debug: print("np_unique_nan: (lst.dtype.kind == 'f')")
                        lst_unique = np.unique(lst.astype(float))
                        # if debug: print("np_unique_nan: lst_unique predfinal:", lst_unique)
                        # lst_unique = np.array(list(set(list(lst))))
                        # if debug: print("np_unique_nan: lst_unique predfinal v2:", lst_unique)
                        # if np.isnan(lst).all():
                        #     lst_unique = np.nan
                        #     if debug: print("np_unique_nan: lst_unique predfinal v3:", lst_unique)
                    elif lst.dtype == object:
                        # if debug: print("np_unique_nan: lst.dtype == object")
                        lst_unique = np.array(list(set(list(lst))))
                    else:
                        if debug: print("np_unique_nan: else 0")
                        lst_unique = np.unique(lst)
                else:
                    if debug: print('np_unique_nan:','else i...')
                    lst_unique = np.array(list(set(lst)))
                    
            elif len(data_types_set) == 0:
                lst_unique = None
            else:
                # print('else')
                lst_unique = np.array(list(set(lst)))
        else: # другой тип данных
            if debug: print('np_unique_nan:','другой тип данных')
            # lst_unique = np.unique(np.array(list(set(lst)),dtype=object))
            # lst_unique = np.unique(np.array(list(set(lst)))) # Исходим из того что все елеменыт спсика одного типа
            lst_unique = lst
    if type(lst_unique) == np.ndarray:
        if lst_unique.shape[0]==1: 
            lst_unique = lst_unique[0]
            if (type(lst_unique) == np.ndarray) and (lst_unique.shape[0]==1):  # двойная вложенность
                lst_unique = lst_unique[0]
        elif lst_unique.shape[0]==0: lst_unique = None
    
    return lst_unique

def to_float(value):
    #обсобенность [nan, 10, None] переводит [10. nan] т.е частично делает unique
    float_value = None
    if ((type(value)==str) or (type(value)==np.str_)): # основной сценарий
        try:
            float_value = float(value)
        except:
            float_value = value 
    elif ((type(value)==list) or (type(value)==np.ndarray)):
        # print("elif ((type(value)==list) or (type(value)==np.ndarray))")
        float_value = []
        for v in value:
            # if v is not None and not (((type(v)==float) or (type(v)== np.float64)) and np.isnan(v)):
            if v is not None:
                try:
                    float_value.append(float(v))
                except:
                    float_value.append(v)
            else: float_value.append(np.nan)
        # print("float_value: step 1", float_value)
        # data_types_set = list(set([type(i) for i in value]))
        data_types_set2 = list(set([type(i) for i in float_value]))
        if len(data_types_set2) > 1: # несколько типов данных
            float_value = np.array(float_value, dtype = object)
        elif len(data_types_set2) == 1: # один тип данных
            float_value = np.array(float_value)
        else: 
            float_value = None
    else:
        try:
            float_value = float(value)
        except:
            float_value = value # пока так чтобы не попортить

    return float_value


def to_float_02(value):
    float_value = None
    if ((type(value)==str) or (type(value)==np.str_)):
        try:
            float_value = float(value)
        except:
            float_value = value # пока так чтобы не попортить
            print("to_float: Exception: type(value), value: ", type(value), value)
    elif ((type(value)==list) or (type(value)==np.ndarray)):
        # print("to_float: type(value)==np.ndarray:  type(value), value: ", type(value), value)
        # for v in value:
        #     print(v)
        float_value = np.unique(np.array([float(v) for v in value if v is not None and not (((type(v)==float) or (type(v)== np.float64)) and math.isnan(v) )]))
    else:
        if value is None or (((type(value)==float) or (type(value)==np.float64)) and math.isnan(value)):
            float_value = value
        else:
            print("to_float: type(value), value: ", type(value), value)
            sys.exit(2)

    return float_value

def np_unique_nan_01(lst: np.array)->np.array: # a la version 2.4
    lst_for_unique = lst
    for i_it, item in enumerate(lst_for_unique):
        if type(item)==float and math.isnan(item): 
            lst_for_unique[i_it] = ''
    return np.unique(lst_for_unique)
def np_unique_nan_02 (lst: np.array, debug = False)->np.array: # a la version 2.4
    # lst_for_unique = lst
    if math.nan in lst or None in lst:
        if debug: print('np_unique_nan:', 'if math.nan in lst or None in lst')
        lst_unique = [p for p in lst 
                            if p is not None and not (((type(p)==float) or (type(p)==np.float64)) and math.isnan(p) )]
        lst_unique = np.unique(np.array(list(set(lst_unique)),dtype=object)) # + math.nan
        if  [math.nan] in lst_unique: pass
        else: np.hstack((lst_unique, [math.nan]))
    elif lst is None or (((type(lst)==float) or (type(lst)==np.float64)) and math.isnan(lst)):
        if debug: print('np_unique_nan:','lst is None or (((type(lst)==float) or (type(lst)==np.float64)) and math.isnan(lst))')
        lst_unique = lst
    else:
        data_types_set = list(set([type(i) for i in lst]))
        if debug: print('np_unique_nan:', 'data_types_set:', data_types_set)
        if ((type(lst)==list) or (type(lst)==np.ndarray)):
            if debug: print('np_unique_nan:','if ((type(lst)==list) or (type(lst)==np.ndarray)):')
            # lst_unique = np.unique(lst)
            # TypeError: '<' not supported between instances of 'str' and 'float'
            # lst_unique = list(set(lst))
            
            # lst_unique = np.unique(lst)
            # TypeError: unhashable type: 'list'
            if len(data_types_set) > 1: # несколько типов данных
                lst_unique = np.array(list(set(lst)), dtype=object)
            elif list in data_types_set:
                lst_unique = np.unique(np.array(lst))
                # TypeError: unhashable type: 'list'
            elif  np.ndarray in data_types_set :
                # lst_unique = np.array(lst)
                lst_unique = np.unique(lst)
            else:
                lst_unique = np.array(list(set(lst)))
        else:
            if debug: print('np_unique_nan:','else if ((type(lst)==list) or (type(lst)==np.ndarray)):')
            # lst_unique = np.unique(np.array(list(set(lst)),dtype=object))
            lst_unique = np.unique(np.array(list(set(lst)))) # Исходим из того что все елеменыт спсика одного типа
    if lst_unique.shape[0]==1: 
        lst_unique = lst_unique[0]
        if (type(lst_unique) == np.ndarray) and (lst_unique.shape[0]==1):  # двойная вложенность
            lst_unique = lst_unique[0]
    elif lst_unique.shape[0]==0: lst_unique = None

    # for i_it, item in enumerate(lst_for_unique):
    #     if type(item)==float and math.isnan(item): 
    #         lst_for_unique[i_it] = ''
    return lst_unique
def np_unique_nan_wrapper(lst: np.array)->np.array: 
    lst = np_unique_nan_01(lst)
    if lst.shape[0]==1: lst = lst[0]
    if (type(lst) == list or type(lst) == np.ndarray) and len(lst) == 0: lst  = None
    return lst
def np_unique_nan_ext(lst: np.array)->np.array: # a la version 2.4
    # для разных типов данных проверяем т.е. в спсике могут быть строки списки, словари и что-нибудь еще
    data_types = [type(i) for i in lst]
    data_types_set = list(set(data_types))
    unique_lst = []
    for data_type in data_types_set:
        # for idt, d_t in enumerate(data_types):
        lst_for_unique_lst = np.array([lst[idt] for idt, d_t in enumerate(data_types) if data_type==d_t], dtype=object)
        unique_lst.append(np_unique_nan_01(lst_for_unique_lst))
        # unique_lst.append(np_unique_nan_wrapper(lst_for_unique_lst))
    # return np.unique(unique_lst)
    return unique_lst   
def np_array_wrapper(lst: np.array)->np.array: 
    if lst is None: return None
    if ((type(lst) == list) or (type(lst) == np.ndarray)):
        if len(lst)==1: lst = lst[0]
        elif len(lst) == 0: lst  = None
        else: pass
    return lst  

def test_esklp_by_n_ru_bar_code(mnn_standard_z, trade_name_z, n_ru_z, bar_code_z, data_reg_price_z, is_reg_price=True, debug = False):
    # global smnn_list_df, klp_list_dict_df, zvnlp_df
    # if 'klp_list_dict_df' in globals(): print ("'klp_list_dict_df' in globals")
    is_n_ru_in_ESKLP, is_barcode_in_ESKLP, is_n_ru_and_barcode_in_ESKLP, code_klp_lst = False, False, False, np.array([])
    # code_klp_lst берем пока только по штрих-коду без №_ru
    # global klp_list_dict_df
    if mnn_standard_z is None or trade_name_z is None or n_ru_z is None or bar_code_z is None \
        or not ((type(mnn_standard_z)==str) or (type(mnn_standard_z)==np.str_)):
        return False, False, None, None
    bar_code_list = bar_code_z.split(',')

    # варинт 0 поиск только по штрих-коду - типа у одного штрихкода не м.б. несколько ТН (в одно время туту еще будем сотмреть дату решения)
    # mask_bar_code = (klp_list_dict_df['lim_price_barcode_str'].notnull()) & \
    #     (klp_list_dict_df['lim_price_barcode_str'].str.contains(bar_code_z))              
    # bar_code_srch_list = '|'. join([r"(?:" + bar_code + r")" for bar_code in bar_code_list])
    # bar_code_srch_list = '(' + ' or '. join([f"{bar_code} in lim_price_barcode_str" for bar_code in bar_code_list]) + ')'
    # bar_code_srch_list = ' or '. join([f"'{bar_code}' in lim_price_barcode_str" for bar_code in bar_code_list])
    # klp_list_dict_df[ne.evaluate(f"contains(np_bar_code_str, '{bar_code_z}') | contains(np_bar_code_str, '{bar_code_z2}')")].shape #!ok
    # np_lim_price_barcode_str = g.np_lim_price_barcode_str

    bar_code_srch_list = '|'. join([f"contains(np_lim_price_barcode_str, '{bar_code}')" for bar_code in bar_code_list])
    
    if debug: print(f"test_esklp_by_n_ru_bar_code: bar_code_srch_list: '{bar_code_srch_list}'")
    # варинт 0 усовершенствованный поиск только по штрих-коду и дате решения по цене из ЖНВЛП
    if is_reg_price:
        # np_lim_price_reg_date_str = g.np_lim_price_reg_date_str
        # query_str = bar_code_srch_list + ' and @data_reg_price_z in lim_price_reg_date_str'
        query_str = bar_code_srch_list + f" & contains(np_lim_price_reg_date_str, '{data_reg_price_z}')"
        # code_klp_lst_pre = klp_list_dict_df.query(query_str)[['code_klp','lim_price_barcode', 'lim_price_reg_date']].values
        code_klp_lst_pre = klp_list_dict_df[ne.evaluate(query_str)][['code_klp','lim_price_barcode', 'lim_price_reg_date']].values
        # code_klp_lst = np.empty(code_klp_lst_pre.shape[0])
        # code_klp_lst = np.empty(0)
        code_klp_lst = np.array([])
        if debug: print(f"test_esklp_by_n_ru_bar_code: code_klp_lst_pre.shape:",  code_klp_lst_pre.shape)
        lp_bar_codes_pre = code_klp_lst_pre[:,1]
        lp_reg_dates_pre = code_klp_lst_pre[:,2]
        if debug: print(f"test_esklp_by_n_ru_bar_code: lp_bar_codes_pre", lp_bar_codes_pre)
        if debug: print(f"test_esklp_by_n_ru_bar_code: lp_reg_dates_pre", lp_reg_dates_pre)
        if code_klp_lst_pre.shape[0]>0: # одна или несколько строк
            for k, code_klp_pre in enumerate(code_klp_lst_pre):
                if debug: print(f"test_esklp_by_n_ru_bar_code: code_klp_pre", code_klp_pre)
                # внтури тип данных list или str
                # if debug: print(f"test_esklp_by_n_ru_bar_code: type(code_klp_pre[1]), type(code_klp_pre[2]:", type(code_klp_pre[1]), type(code_klp_pre[2]))
                if type(code_klp_pre[1])==type(code_klp_pre[2]):
                    if (type(code_klp_pre[1])==str or type(code_klp_pre[1])==np.str_):
                        if code_klp_pre[1] in bar_code_list and code_klp_pre[2]== data_reg_price_z:
                            # np.append(code_klp_lst, code_klp_pre[0], axis=0)
                            code_klp_lst = np.hstack((code_klp_lst, code_klp_pre[0]))
                    elif (type(code_klp_pre[1])==list or type(code_klp_pre[1])==np.ndarray):
                        # if debug: print(f"test_esklp_by_n_ru_bar_code: (type(code_klp_pre[1])==list or type(code_klp_pre[1])==np.ndarray)")
                        # if debug: print(f"test_esklp_by_n_ru_bar_code: type(code_klp_pre[1], type(code_klp_pre[2])", type(code_klp_pre[1]), type(code_klp_pre[2]))
                        fl_break = False
                        for i, bar_code in enumerate(bar_code_list):  # type==list
                            for j,(lp_bar_code, lp_reg_date) in enumerate(zip(code_klp_pre[1],code_klp_pre[2])): #[::-1]: # пойдем реверсом с поcледних дат
                                # d ЕСКЛП записывают по разному бывает что ипоследне в первых элементах списка
                                if lp_bar_code==bar_code and lp_reg_date==data_reg_price_z:
                                    if debug: print(f"test_esklp_by_n_ru_bar_code: lp_bar_code==bar_code and lp_reg_date==data_reg_price_z: {k}, {j}", lp_bar_code, lp_reg_date)
                                    code_klp_lst = np.hstack((code_klp_lst, code_klp_pre[0]))
                                    fl_break = True
                                    break
                            if fl_break: break # выскакиваем по первому штрихкоду
        # code_klp_lst = code_klp_lst_pre # для проверки быстродействия
        if debug: print(f"test_esklp_by_n_ru_bar_code: code_klp_lst", code_klp_lst)
        # for i, bar_code in enumerate(bar_code_list):
        #     if np.where(lp_bar_codes, bar_code)
    else:
        query_str = bar_code_srch_list
        # code_klp_lst_pre = klp_list_dict_df.query(query_str)[['code_klp','lim_price_barcode', 'lim_price_reg_date']].values
        code_klp_lst = klp_list_dict_df[ne.evaluate(query_str)]['code_klp'].values
        

    
    if code_klp_lst.shape[0] > 0 :
        is_barcode_in_ESKLP = True
    else: code_klp_lst = None
    is_n_ru_in_ESKLP = None
    is_n_ru_and_barcode_in_ESKLP = None
    return is_n_ru_in_ESKLP, is_barcode_in_ESKLP, is_n_ru_and_barcode_in_ESKLP, code_klp_lst

# v 03
# pd.query Wall time: 44.6 ms
# Wall time: 279-307 ms c np_unique_nan_wrapper
# Wall time: 270-360 ms без np_unique_nan_wrapper
# количество колонок практически не виляет на скорость
# klp_list_dict_df.set_index(['code_klp'], drop=False) не влияет
#  - Кол-во ЕИ ЛП  во вторичной (потребительской) упаковке (10)
#  - Первичная упаковка / Кол-во лекарственной формы (11)
#  - Вторичная (потребительская) упаковка / Кол-во первичных упаковок (13)
def select_klp_by_code_klp (code_klp_lst, return_values_cols_list, debug=False):
    # global smnn_list_df, klp_list_dict_df, zvnlp_df
    if debug: 
        if code_klp_lst is None: 
            print(f"select_klp_by_code_klp: code_klp_lst: {code_klp_lst}")
        else:
            print(f"select_klp_by_code_klp: type(code_klp_lst): {type(code_klp_lst)},",
                    f"len(code_klp_lst): {len(code_klp_lst)}, code_klp_lst[:5]: {code_klp_lst[:5]}", )
    n_cols = len(return_values_cols_list)
    if code_klp_lst is not None and ((type(code_klp_lst)==np.ndarray) or ((type(code_klp_lst)==list))) and  len(code_klp_lst)>0:
    # if code_klp_lst is not None and ((type(code_klp_lst)==np.ndarray) and  (code_klp_lst.shape[0] > 0)):
        # srch_list = '|'.join([r"(?:" + code_klp + r")" for code_klp in code_klp_lst])
        # bar_code_srch_list = ' or '. join([f"'{bar_code}' in lim_price_barcode_str" for bar_code in bar_code_list])
        # return_values_pre = klp_list_dict_df[klp_list_dict_df['code_klp'].str.contains(srch_list, regex=True)][return_values_cols_list].values
        query_str = ' or '. join([f"code_klp == '{code_klp}'" for code_klp in code_klp_lst])
        # query_str = f"code_klp == {code_klp_lst}"
        # ValueError: multi-line expressions are only valid in the context of data, use DataFrame.eval
        if debug: print(f"select_klp_by_code_klp: query_str: '{query_str}'")
        return_values_pre = klp_list_dict_df.query(query_str)[return_values_cols_list].values
        if debug: print(f"select_klp_by_code_klp: step1: return_values_pre.shape", return_values_pre.shape, return_values_pre )
        return_values = []
        for i in range(n_cols):
            # lst = np_unique_nan_wrapper(return_values_pre[:,i])
            lst = np_unique_nan(return_values_pre[:,i], debug=debug)
            return_values.append(lst)
            if debug: print(f"select_klp_by_code_klp: i: {i}, lst.dtype.kind: {lst.dtype.kind}, {lst.dtype},  lst: {lst}")
        
        return_values = np.array(return_values, dtype=object)
        # return_values = return_values_pre # для проверки быстродействия
    else: return_values = np.array(n_cols * [None])
    if debug: print(f"select_klp_by_code_klp: step3: return_values.shape/values", return_values.shape, return_values )
    return return_values    

# Wall time: 93.5 µs
def extract_dosage_standard(dosage_standard_value_str, debug=False):
    # на входе: # : '300 ЛЕ/мл'
    # update: варинаты: "10 мг, 20 мг, 30 мг" / '8-15 млн/мг' // '250 мг+62.5 мг/5 мл' / '62.5 мг/5 мл' / '1 доза/0.5 мл' / "['2 мг' '5 мг']"
    dosage_standard_value, dosage_standard_unit, pseudo_vol = None, None, None
    # print("dosage_standard_value_str: {dosage_standard_value_str}")
    if dosage_standard_value_str is not None:
        try: #'numpy.ndarray'  and not (type(dosage_standard_value_str)==str)
            if (not (type(dosage_standard_value_str)==np.ndarray)) \
                and not (dosage_standard_value_str=='~') \
                and not (dosage_standard_value_str.lower()=='не указано') \
                and not ('+' in dosage_standard_value_str) \
                and not (', ' in dosage_standard_value_str) \
                and not ("' '" in dosage_standard_value_str) \
                and re.search(r"(\d+\.*-\d*)", dosage_standard_value_str) is None: # не сложная дозировка
                # print('не сложная жохировка')
                if re.search(r"/\s*\d+\.*\d*" , dosage_standard_value_str) is not None: # есть цифра псевдообъема 50 мг/5 мл
                    dosage_standard_value =  float(re.sub (r"[^(\d*\.\d*)]", '', re.sub(r"(/.*)", '', dosage_standard_value_str)))
                    dosage_standard_unit = re.sub(r"[(\d*\.\d*)]", '', dosage_standard_value_str[:dosage_standard_value_str.rfind('/')]).strip() + \
                        re.sub(r"[\d\.\,\s]", '', dosage_standard_value_str[dosage_standard_value_str.rfind('/'):].strip()).strip()
                    try:
                        pseudo_vol  = float(re.sub(r"[^\d\.\,]", '', dosage_standard_value_str[dosage_standard_value_str.rfind('/')+1:].strip()))
                    except Exception as err:
                        print(err)
                else: 
                    dosage_standard_value = float(re.sub (r"[^(\d*\.\d*)]",'', dosage_standard_value_str))
                    dosage_standard_unit = re.sub (r"[(\d*\.\d*)]",'', dosage_standard_value_str).strip() 
                    pseudo_vol = 1.0
                # dosage_standard_value = float(re.sub (r"(\d*([\.\-])*\d+)",'', dosage_standard_value_str))
                # dosage_standard_unit = re.sub (r"(\d*([\.\-])*\d+)",'', dosage_standard_value_str).strip() 
                # не србатывает при '10000 анти-Ха ЕД/мл'
        except Exception as err:
            # print(f"select_dosage_standard: dosage: {dosage}")
            print(f"select_dosage_standard: type(dosage_standard_value_str): {type(dosage_standard_value_str)}")
            print(f"select_dosage_standard: dosage_standard_value_str: {dosage_standard_value_str}")
    # return dosage_standard_value_str, dosage_standard_value, dosage_standard_unit
    return dosage_standard_value, dosage_standard_unit, pseudo_vol

    # def extract_dosage_standard(dosage_standard_value_str, debug=False):
    #     # на входе: # '{'grls_value': '300 ЛЕ/мл', 'dosage_unit': {'name': 'ЛЕ/мл', 'okei_code': '876', 'okei_name': 'усл. ед'} 
    #     # '300 ЛЕ/мл'
    #     dosage_standard_value, dosage_standard_unit = None, None
    #     # if dosage is not None and type(dosage)==dict:
    #     #     dosage_standard_value_str = dosage.get('grls_value')
    #     if debug: print(f"extract_dosage_standard: type(dosage_standard_value_str): {type(dosage_standard_value_str)}, dosage_standard_value_str:{dosage_standard_value_str}")
    #     if dosage_standard_value_str is not None:
    #         try: #'numpy.ndarray'  and not (type(dosage_standard_value_str)==str)
    #             if (not (type(dosage_standard_value_str)==np.ndarray)) \
    #               and not (dosage_standard_value_str=='~') \
    #               and not (dosage_standard_value_str.lower()=='не указано') \
    #               and not ('+' in dosage_standard_value_str) \
    #               and not (', ' in dosage_standard_value_str) \
    #               and re.search(r"(\d+\.*-\d*)", dosage_standard_value_str) is None: # не сложная дозировка '8-15 млн/мг'
    #                 dosage_standard_value = float(re.sub (r"[^(\d*\.\d*)]",'', dosage_standard_value_str))
    #                 dosage_standard_unit = re.sub (r"[(\d*\.\d*)]",'', dosage_standard_value_str).strip() 
    #                 # dosage_standard_value = float(re.sub (r"(\d*([\.\-])*\d+)",'', dosage_standard_value_str))
    #                 # dosage_standard_unit = re.sub (r"(\d*([\.\-])*\d+)",'', dosage_standard_value_str).strip() 
    #                 # не србатывает при '10000 анти-Ха ЕД/мл'
    #         except Exception as err:
    #             # print(f"select_dosage_standard: dosage: {dosage}")
    #             print(f"select_dosage_standard: type(dosage_standard_value_str): {type(dosage_standard_value_str)}")
    #             print(f"select_dosage_standard: dosage_standard_value_str: {dosage_standard_value_str}")
    #     # return dosage_standard_value_str, dosage_standard_value, dosage_standard_unit
    #     return dosage_standard_value, dosage_standard_unit

def extract_dosage_standard_wrapper(dosage_standard_value_str, debug=False):
    dosage_standard_value, dosage_standard_unit, pseudo_vol = None, None, None
    if dosage_standard_value_str is None: return dosage_standard_value, dosage_standard_unit, pseudo_vol
    elif type(dosage_standard_value_str)==str or type(dosage_standard_value_str)==np.str_:
        # dosage_standard_value, dosage_standard_unit = extract_dosage_standard(dosage_standard_value_str, debug=debug)
        dosage_standard_value, dosage_standard_unit, pseudo_vol = extract_dosage_standard(dosage_standard_value_str, debug=debug)
    elif (type(dosage_standard_value_str) == list or type(dosage_standard_value_str)==np.ndarray):
        dosage_standard_value, dosage_standard_unit, pseudo_vol = [], [], []
        for doze_str in dosage_standard_value_str:
            dosage_standard_value_pre, dosage_standard_unit_pre, pseudo_vol_pre = extract_dosage_standard(doze_str, debug=debug)
            dosage_standard_value.append(dosage_standard_value_pre)
            dosage_standard_unit.append(dosage_standard_unit_pre)
            pseudo_vol.append(pseudo_vol_pre)
        # dosage_standard_value, dosage_standard_unit = np.array(set(dosage_standard_value),dtype=object), np.array(set(dosage_standard_unit),dtype=object)
        dosage_standard_value, dosage_standard_unit = \
            np.array(list(set(dosage_standard_value)),dtype=object), np.array(list(set(dosage_standard_unit)),dtype=object)
        if math.nan in pseudo_vol or None in pseudo_vol:
            pseudo_vol = [p for p in pseudo_vol if p is not None and not ((type(p)==float) and math.isnan(p) )]
            pseudo_vol = np.unique(np.array(list(set(pseudo_vol)),dtype=object)) + [math.nan]
        else:
            pseudo_vol = np.unique(np.array(list(set(pseudo_vol)),dtype=object))
        if dosage_standard_value.shape[0]==1: dosage_standard_value = dosage_standard_value[0]
        elif dosage_standard_value.shape[0]==0: dosage_standard_value = None
        if dosage_standard_unit.shape[0]==1: dosage_standard_unit = dosage_standard_unit[0]
        elif dosage_standard_unit.shape[0]==0: dosage_standard_unit = None
        if pseudo_vol.shape[0]==1: pseudo_vol = pseudo_vol[0]
        elif pseudo_vol.shape[0]==0: pseudo_vol = None
        
    # return dosage_standard_value, dosage_standard_unit
    return dosage_standard_value, dosage_standard_unit, pseudo_vol
    # Wall time: 9.06 µs
    # def extract_dosage_standard_wrapper(dosage_standard_value_str, debug=False):
    #     dosage_standard_value, dosage_standard_unit = None, None
    #     if dosage_standard_value_str is None: return dosage_standard_value, dosage_standard_unit
    #     elif type(dosage_standard_value_str)==str or type(dosage_standard_value_str)==np.str_:
    #         dosage_standard_value, dosage_standard_unit = extract_dosage_standard(dosage_standard_value_str, debug=debug)
    #     elif (type(dosage_standard_value_str) == list or type(dosage_standard_value_str)==np.ndarray):
    #         dosage_standard_value, dosage_standard_unit = [], []
    #         for doze_str in dosage_standard_value_str:
    #             dosage_standard_value_pre, dosage_standard_unit_pre = extract_dosage_standard(doze_str, debug=debug)
    #             dosage_standard_value.append(dosage_standard_value_pre)
    #             dosage_standard_unit.append(dosage_standard_unit_pre)
    #         # dosage_standard_value, dosage_standard_unit = np.array(set(dosage_standard_value),dtype=object), np.array(set(dosage_standard_unit),dtype=object)
    #         dosage_standard_value, dosage_standard_unit = np.array(list(set(dosage_standard_value)),dtype=object), np.array(list(set(dosage_standard_unit)),dtype=object)
    #     # return dosage_standard_value, dosage_standard_unit
    #     return np_array_wrapper(dosage_standard_value), np_array_wrapper(dosage_standard_unit)        

# Wall time: 13.2 ms
def select_smnn_by_code_smnn (code_smnn_lst, return_values_cols_list, debug=False):
    # global smnn_list_df, klp_list_dict_df, zvnlp_df
    n_cols = len(return_values_cols_list)
    return_values = np.array(n_cols * [None])
    if code_smnn_lst is not None:
        if type(code_smnn_lst) == str or type(code_smnn_lst)==np.str_:
            srch_list = r"(?:" + code_smnn_lst + r")"
        elif ((type(code_smnn_lst) == list) or (type(code_smnn_lst)==np.ndarray)) and  len(code_smnn_lst) > 0:
            srch_list = '|'.join([r"(?:" + code_smnn + r")" for code_smnn in code_smnn_lst])
        else: srch_list = None
        if debug: print(f"select_smnn_by_code_smnn: srch_list: {srch_list}")
        if srch_list is not None:
            return_values_pre = smnn_list_df[smnn_list_df['code_smnn'].str.contains(srch_list, regex=True)][return_values_cols_list].values
            if debug: print(f"select_smnn_by_code_smnn: return_values_pre.shape", return_values_pre.shape, return_values_pre[:5] )
            return_values = []
            for i in range(n_cols):
                # lst = np_unique_nan_wrapper(np_unique_nan_ext(return_values_pre[:,i]))
                lst = np_unique_nan(return_values_pre[:,i], debug=False)
                # lst = np_array_wrapper(np.array(return_values_pre[:,i], dtype=object))
                return_values.append(lst)
                # if debug: print(f"select_klp_by_code_klp: i: {i}, lst: {lst}")
            return_values = np.array(return_values, dtype=object)
        # else: return_values = np.array(n_cols * [None])
    # else: return_values = np.array(n_cols * [None])
    return return_values    

def extract_lim_price_n_data_design ( ru_z):
    # это для экстракции даты и № решения о цене
    if ru_z is not None: 
        ru_z_lst = ru_z.split('\n')
        ru_z_lst = [it for it in ru_z_lst if len (it) >0] 
        #  исключаем ситуацию когда несколько раз ставят \n вместо одного
        try:
            data_ru_z, n_ru_z = ru_z_lst
            data_ru_z = data_ru_z.strip()
            n_ru_z = re.sub(r"\(|\)", '', n_ru_z).strip()
        except Exception as err:
            print( err)
            print(f"ru_z_lst: {ru_z_lst}, ru_z: {ru_z}")
            data_ru_z, n_ru_z = None, None
    else: data_ru_z, n_ru_z = None, None
    return data_ru_z, n_ru_z

def print_debug_znvlp(mnn_standard_z, trade_name_z, n_ru_z, bar_code_z,
            data_reg_price_z, n_reg_price_z,
            is_n_ru_in_ESKLP, is_barcode_in_ESKLP, is_n_ru_and_barcode_in_ESKLP, 
            code_smnn, code_klp, mnn_standard, trade_name, 
            form_standard, form_standard_unify, doze_group,
            dosage_standard_value_str,  dosage_standard_value, dosage_standard_unit, pseudo_vol,
            pack_1_num, pack_1_name,	pack_2_num,	pack_2_name, consumer_total,
            ls_unit_okei_name, ls_unit_name, 
            # ath, 
            ath_name, ath_code, is_znvlp, is_narcotic,
            is_dosed, mass_volume_num, mass_volume_name, # 08/11/2022
            manufacturer_name, manufacturer_country_code, manufacturer_country_name, manufacturer_address
            
    ):
    print(f"mnn_standard_z: '{mnn_standard_z}', trade_name_z: '{trade_name_z}', n_ru_z: '{n_ru_z}', bar_code_z: '{bar_code_z}'")
    # print(f"is_n_ru_in_ESKLP: {is_n_ru_in_ESKLP}, is_barcode_in_ESKLP: {is_barcode_in_ESKLP}, is_n_ru_and_barcode_in_ESKLP: {is_n_ru_and_barcode_in_ESKLP}")
    print(f"data_reg_price_z: {data_reg_price_z}, n_reg_price_z: {n_reg_price_z}")
    print(f"code_smnn: {code_smnn}, code_klp: {code_klp}")
    print(f"mnn_standard KLP: {mnn_standard}, trade_name KLP: {trade_name}")
    print(f"form_standard: {form_standard}, form_standard_unify: {form_standard_unify}, doze_group: {doze_group}")
    print(f"pack_1_num: {pack_1_num}, pack_1_name: {pack_1_name}, pack_2_num: {pack_2_num}, pack_2_name: {pack_2_name}, consumer_total: {consumer_total}")
    print(f"dosage_standard_value_str: '{dosage_standard_value_str}', ls_unit_okei_name: '{ls_unit_okei_name}', ls_unit_name: '{ls_unit_name}'")
    print(f"dosage_standard_value: {dosage_standard_value}, dosage_standard_unit: {dosage_standard_unit}, pseudo_vol: {pseudo_vol}")
    # print(f"ath: {ath}, ath_name: {ath_name}, ath_code: {ath_code}, is_znvlp: {is_znvlp}, is_narcotic: {is_narcotic}")
    print(f"ath_name: {ath_name}, ath_code: {ath_code}, is_znvlp: {is_znvlp}, is_narcotic: {is_narcotic}")
    print(f"is_dosed: {is_dosed}, mass_volume_num: '{mass_volume_num}', mass_volume_name: '{mass_volume_name}'")
    print(f"manufacturer_name: {manufacturer_name}, m_country_code: {manufacturer_country_code}, m_country_name: {manufacturer_country_name}, m_address: {manufacturer_address}" )
    
    print()

def extend_znvlp(mnn_standard_z, trade_name_z, n_ru_z, bar_code_z, reg_price_z, proc_tag='lp_date', is_reg_price=True, 
                 debug=False, debug_print=True):
    # global smnn_list_df, klp_list_dict_df, zvnlp_df
    if is_reg_price:
        data_reg_price_z, n_reg_price_z = extract_lim_price_n_data_design (reg_price_z)
        # transform str 06.04.2016 to str 2016-04-06
        try:
            date_c = datetime.datetime.strptime(data_reg_price_z, "%d.%m.%Y").date()
        except Exception as err:
            date_c = None
            print(err)
            print(mnn_standard_z, trade_name_z, n_ru_z, bar_code_z, reg_price_z)
        data_reg_price_z_format = datetime.datetime.strftime(date_c, "%Y-%m-%d")
        if debug: print(f"extend_znvlp: data_reg_price_z: {data_reg_price_z}, n_reg_price_z: {n_reg_price_z}, data_reg_price_z_format: {data_reg_price_z_format}")
        is_n_ru_in_ESKLP, is_barcode_in_ESKLP, is_n_ru_and_barcode_in_ESKLP, code_klp = \
            test_esklp_by_n_ru_bar_code(mnn_standard_z, trade_name_z, n_ru_z, bar_code_z, data_reg_price_z_format, is_reg_price, debug=debug)
        # is_n_ru_in_ESKLP, is_barcode_in_ESKLP, is_n_ru_and_barcode_in_ESKLP, code_klp_lst = False, False, False,None
    else:
        data_reg_price_z, n_reg_price_z, data_reg_price_z_format = None, None, None
        is_n_ru_in_ESKLP, is_barcode_in_ESKLP, is_n_ru_and_barcode_in_ESKLP, code_klp = \
            test_esklp_by_n_ru_bar_code(mnn_standard_z, trade_name_z, n_ru_z, bar_code_z, data_reg_price_z_format, is_reg_price, debug=debug)

    return_klp_values_cols = ['code_smnn', 'mnn_standard',  'trade_name',
                              'form_standard', 'form_standard_unify',
                              'pack_1_num',	'pack_1_name', 'pack_2_num',	'pack_2_name'	, 'consumer_total', 
                              'is_dosed', 'mass_volume_num', 'mass_volume_name', # 08.11.2022
                              'manufacturer_name', 'manufacturer_country_code', 'manufacturer_country_name', 'manufacturer_address', # 15.11.2022
                               	]
    # добавить 'form_standard_unify',  и определить 'doze_group'
    # добавить code_klp_lst, code_smnn
    # try :
    code_smnn, mnn_standard, trade_name, form_standard, form_standard_unify,  \
    pack_1_num,	pack_1_name,	pack_2_num,	pack_2_name	, consumer_total,\
    is_dosed, mass_volume_num, mass_volume_name,\
    manufacturer_name, manufacturer_country_code, manufacturer_country_name, manufacturer_address =\
        select_klp_by_code_klp (code_klp, return_klp_values_cols, debug=debug)
    
    if debug: print(f"extend_znvlp: after select_klp..: pack_1_num:",
     f"{pack_1_num},	pack_2_num: {pack_2_num},	consumer_total: {consumer_total}, mass_volume_num: {mass_volume_num}, mass_volume_name: {mass_volume_name}")
    pack_1_num = to_float(pack_1_num)
    pack_2_num = to_float(pack_2_num)
    consumer_total = to_float(consumer_total)
    mass_volume_num = to_float(mass_volume_num)
    if debug: print(f"extend_znvlp: after to_float() : pack_1_num:",
     f"{pack_1_num},	pack_2_num: {pack_2_num},	consumer_total: {consumer_total}, mass_volume_num: {mass_volume_num}, mass_volume_name: {mass_volume_name}")
    # except Exception as err:
    #     print(err)
    #     print(f"mnn_standardz: {mnn_standard_z}, trade_name: {trade_name_z}, n_ru: {n_ru_z}, bar_code: {bar_code_z}, data_reg_price: {data_reg_price_z_format}")
    #     # proc_tag = 'format_error'
    #     sys.exit(2)

    doze_group = None
    if type(form_standard_unify)==str or type(form_standard_unify)==np.str_: 
        doze_group_arr = doze_vol_pharm_form_handlers.get(form_standard_unify)
        if doze_group_arr is not None:
            doze_group = doze_group_arr[0]
    else: doze_group = None

    # return_smnn_values_cols_list = ['dosage_standard_value',  'ls_unit_okei_name', 'ls_unit_name',	 
    #                        'ath', 'is_znvlp', 'is_narcotic' ] #'dosage_grls_value', 
    # убрать dosage_grls_value
    # dosage_standard_value_str, ls_unit_okei_name, ls_unit_name, ath, is_znvlp, is_narcotic = \
    #     select_smnn_by_code_smnn (code_smnn, return_smnn_values_cols_list, debug=debug)
    # ath_name, ath_code = extract_ath(ath)
    return_smnn_values_cols_list = ['dosage_standard_value', 'ls_unit_okei_name', 'ls_unit_name',	 
                           'ath_name', 'ath_code', 'is_znvlp', 'is_narcotic' ]                            

    dosage_standard_value_str, ls_unit_okei_name, ls_unit_name, ath_name, ath_code, is_znvlp, is_narcotic = \
        select_smnn_by_code_smnn (code_smnn, return_smnn_values_cols_list, debug=debug)
    

    # ath_name, ath_code = None, None
    dosage_standard_value, dosage_standard_unit, pseudo_vol = extract_dosage_standard_wrapper(dosage_standard_value_str, debug=debug)
    # dosage_standard_value, dosage_standard_unit = None, None
    if code_klp is None: proc_tag = None
    if debug_print: 
        print_debug_znvlp(mnn_standard_z, trade_name_z, n_ru_z, bar_code_z,
            data_reg_price_z, n_reg_price_z,
            is_n_ru_in_ESKLP, is_barcode_in_ESKLP, is_n_ru_and_barcode_in_ESKLP, 
            code_smnn, np_array_wrapper(code_klp), mnn_standard, trade_name, 
            form_standard, form_standard_unify, doze_group,
            dosage_standard_value_str,  dosage_standard_value, dosage_standard_unit, pseudo_vol,
            pack_1_num, pack_1_name,	pack_2_num,	pack_2_name, consumer_total,
            ls_unit_okei_name, ls_unit_name, 
            # ath, ath_name, ath_code, is_znvlp, is_narcotic 
            ath_name, ath_code, is_znvlp, is_narcotic,
            is_dosed, mass_volume_num, mass_volume_name,
            manufacturer_name, manufacturer_country_code, manufacturer_country_name, manufacturer_address
        )
    return is_n_ru_in_ESKLP, is_barcode_in_ESKLP, is_n_ru_and_barcode_in_ESKLP,\
            code_smnn, np_array_wrapper(code_klp), mnn_standard, trade_name, \
            form_standard, form_standard_unify, doze_group,\
            dosage_standard_value_str,  dosage_standard_value, dosage_standard_unit, pseudo_vol,\
            pack_1_num, pack_1_name,	pack_2_num,	pack_2_name, consumer_total,\
            ls_unit_okei_name, ls_unit_name, \
            ath_name, ath_code, is_znvlp, is_narcotic, \
            is_dosed, mass_volume_num, mass_volume_name,\
            manufacturer_name, manufacturer_country_code, manufacturer_country_name, manufacturer_address, \
            proc_tag            

def test_extend_znvlp(i_row, is_reg_price=True, debug=False, debug_print=True, write=False ):
    global smnn_list_df, klp_list_dict_df, zvnlp_df
    # bug 65, 39 НАБОР КАПСУЛ С ПОРОШКОМ ДЛЯ ИНГАЛЯЦИЙ => form_standard_unify = капсулы; НАБОР ТАБЛЕТОК, ПОКРЫТЫХ ОБОЛОЧКОЙ => form_standard_unify = таблетки
    # строки 583, 584, 931
    cols_to_extract = ['МНН','Торговое наименование лекарственного препарата', 'Дата регистрации цены\n(№ решения)', '№ РУ', 'Штрих-код (EAN13)']
    # i_row, cnt, debug, debug_print, write = 582, 0, False, True, False
    # i_row, cnt, debug, debug_print, write = 582, 0, False, True, True
    # i_row, cnt, debug, debug_print, write = 0, 0, False, True, True
    cnt = 0 
    # display(pd.DataFrame([zvnlp_df.loc[i_row,zvnlp_df.columns]]))
    mnn_standard_z, trade_name_z, reg_price_z, n_ru_z, bar_code_z = zvnlp_df.loc[i_row, cols_to_extract].values
    # i_row, doze_group, dosage_standard_value_str, cnt, debug, write = 0, 6, '10 мг, 20 мг, 30 мг', 0, True, False # 
    print(i_row, mnn_standard_z, trade_name_z, reg_price_z, n_ru_z, bar_code_z)
    if is_reg_price: proc_tag ='lp_date'
    else: proc_tag ='lp_no_date'

    _ = extend_znvlp(mnn_standard_z, trade_name_z, n_ru_z, bar_code_z, reg_price_z, proc_tag=proc_tag, is_reg_price=is_reg_price, debug=debug, debug_print=debug_print)

def read_znvlp(path_znvlp_work, fn_znvlp_xlsx, b=0, e=np.inf):
    global smnn_list_df, klp_list_dict_df, zvnlp_df
    path_znvlp = os.path.join(path_znvlp_work, fn_znvlp_xlsx)
    if os.path.exists(path_znvlp):
        logging.info('Reading ZNVLP ' + fn_znvlp_xlsx + ' start...')
        zvnlp_stat_df = pd.read_excel(path_znvlp, nrows=1, )
        znvlp_header = zvnlp_stat_df.columns[0]
        znvlp_date = znvlp_header.split()[-1]
        if znvlp_date is not None: znvlp_date = re.sub(r'\)','', znvlp_date)
        
        zvnlp_df = pd.read_excel(path_znvlp, header=2)
        logger.info('Reading ZNVLP ' + fn_znvlp_xlsx + ' done!')
        if (e==np.inf) or (e==None): e = zvnlp_df.shape[0]
        if (b<0) or b is None: b=0
        zvnlp_df = zvnlp_df[b:e]
        logger.info('ZNVLP от ' + str(znvlp_date) + ', shape: ' + str(zvnlp_df.shape))
    else: 
        logger.error('Reading ZNVLP failed')
        sys.exit(2)

    return zvnlp_df, znvlp_date

def init_extend_znvlp():
    global np_lim_price_barcode_str, np_lim_price_reg_date_str, klp_list_dict_df
    # np_lim_price_barcode_str = klp_list_dict_df['lim_price_barcode_str'].to_numpy(bytes)
    # np_lim_price_reg_date_str =  klp_list_dict_df['lim_price_reg_date_str'].to_numpy(bytes)
    np_lim_price_barcode_str = klp_list_dict_df['lim_price_barcode_str'].astype("string").to_numpy(bytes)
    np_lim_price_reg_date_str =  klp_list_dict_df['lim_price_reg_date_str'].astype("string").to_numpy(bytes)

    # np_lim_price_barcode_str = klp_list_dict_df['lim_price_barcode_str'].to_numpy()
    # np_lim_price_reg_date_str =  klp_list_dict_df['lim_price_reg_date_str'].to_numpy()

def apply_p1_lp_date(b= 0, e = None):
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date
    source_cols = ['МНН', 'Торговое наименование лекарственного препарата',
       'Лекарственная форма, дозировка, упаковка (полная)',
       'Владелец РУ/производитель/упаковщик/Выпускающий контроль', 'Код АТХ',
       'Коли-\nчество в потреб. упаков-\nке', 'Предельная цена руб. без НДС',
       'Цена указана для первич. упаковки', '№ РУ',
       'Дата регистрации цены\n(№ решения)', 'Штрих-код (EAN13)',
       'Дата вступления в силу']
    cols_for_parsing = ['МНН', 'Торговое наименование лекарственного препарата', '№ РУ','Штрих-код (EAN13)','Дата регистрации цены\n(№ решения)']
    new_cols = [
        'is_n_ru_in_ESKLP', 'is_barcode_in_ESKLP', 'is_n_ru_and_barcode_in_ESKLP', 
        'code_smnn', 'code_klp', 'mnn_standard', 'trade_name',
        'form_standard', 'form_standard_unify', 'doze_group',
        'dosage_standard_value_str', 'dosage_standard_value', 'dosage_standard_unit', 'pseudo_vol',
        'pack_1_num', 'pack_1_name', 'pack_2_num',	'pack_2_name', 'consumer_total',
        'ls_unit_okei_name', 'ls_unit_name', 
        'ath_name', 'ath_code', 'is_znvlp', 'is_narcotic', 
        'is_dosed', 'mass_volume_num', 'mass_volume_name', # 08.11.2022
        'manufacturer_name', 'manufacturer_country_code', 'manufacturer_country_name', 'manufacturer_address', # 15.11.2022
        'proc_tag'
    ]
    if e is None or (e < 0) or (e > zvnlp_df.shape[0]):
        e = zvnlp_df.shape[0]+1
    # b, e = 0, zvnlp_df.shape[0]+1
    logger.info('P1: lp_date - Унификация по штрихкоду и дате решения о цене -  start...')
    zvnlp_df.loc[b:e, new_cols] = None
    offset = datetime.timezone(datetime.timedelta(hours=3))
    # print(f"start: {datetime.datetime.now(offset).strftime('%Y_%m_%d %H:%M:%S')}")
    begin_time = datetime.datetime.now(offset)
    print(f"b: {b}, e: {e}", zvnlp_df[b:e].shape[0])
    zvnlp_df.loc[b:e,new_cols] = zvnlp_df.loc[b:e, cols_for_parsing].progress_apply(lambda x: pd.Series(\
        extend_znvlp(x[cols_for_parsing[0]], x[cols_for_parsing[1]], x[cols_for_parsing[2]], x[cols_for_parsing[3]], x[cols_for_parsing[4]],\
                     proc_tag='lp_date', debug=False, debug_print=False),\
         index=new_cols),axis=1)
    
    # for i_row, row in tqdm(zvnlp_df[b:e].iterrows(), total = zvnlp_df[b:e].shape[0]):
    #     if i_row < b: continue
    #     if i_row > e: break
    #     x = row
    #     # zvnlp_df.loc[i_row, new_cols] = pd.Series(extend_znvlp(x[cols_for_parsing[0]], x[cols_for_parsing[1]], x[cols_for_parsing[2]], x[cols_for_parsing[3]], x[cols_for_parsing[4]],\
    #     #             proc_tag='lp_date', debug=False, debug_print=False), index=new_cols)
    #     zvnlp_df.loc[i_row, new_cols] = extend_znvlp(row[cols_for_parsing[0]], row[cols_for_parsing[1]], row[cols_for_parsing[2]], row[cols_for_parsing[3]], row[cols_for_parsing[4]],\
    #                 proc_tag='lp_date', debug=False, debug_print=False)
    
    end_time = datetime.datetime.now(offset)
    logger.info('P1: lp_date - Унификация по штрихкоду и дате решения о цене - done!')
    logger.info("Обработано записей (lp_date):" + str(zvnlp_df[zvnlp_df['proc_tag']=='lp_date'].shape[0]))

    calc_time = end_time - begin_time
    calc_time_lst = str(calc_time).split(':')
    calc_time_str = ':'.join([f"{int(float(c)):02d}" for c in calc_time_lst])
    zvnlp_df.attrs['name'] =  'znvlp'
    zvnlp_df.attrs['date'] = znvlp_date
    # zvnlp_df.attrs['date'] = '2022_10_25_1840'
    zvnlp_df.attrs['esklp'] = esklp_date_format #'2022_11_10_active'
    zvnlp_df.attrs['datetime_stamp'] = end_time.strftime("%Y_%m_%d_%H%M")
    zvnlp_df.attrs['calc_time'] = calc_time_str
    
    print(zvnlp_df.attrs)
    

def apply_p2_lp_no_date():
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date
    source_cols = ['МНН', 'Торговое наименование лекарственного препарата',
       'Лекарственная форма, дозировка, упаковка (полная)',
       'Владелец РУ/производитель/упаковщик/Выпускающий контроль', 'Код АТХ',
       'Коли-\nчество в потреб. упаков-\nке', 'Предельная цена руб. без НДС',
       'Цена указана для первич. упаковки', '№ РУ',
       'Дата регистрации цены\n(№ решения)', 'Штрих-код (EAN13)',
       'Дата вступления в силу']
    cols_for_parsing = ['МНН', 'Торговое наименование лекарственного препарата', '№ РУ','Штрих-код (EAN13)','Дата регистрации цены\n(№ решения)']
    new_cols = [
        'is_n_ru_in_ESKLP', 'is_barcode_in_ESKLP', 'is_n_ru_and_barcode_in_ESKLP', 
        'code_smnn', 'code_klp', 'mnn_standard', 'trade_name',
        'form_standard', 'form_standard_unify', 'doze_group',
        'dosage_standard_value_str', 'dosage_standard_value', 'dosage_standard_unit', 'pseudo_vol', 
        'pack_1_num', 'pack_1_name',	'pack_2_num',	'pack_2_name', 'consumer_total',
        'ls_unit_okei_name', 'ls_unit_name', 
        'ath_name', 'ath_code', 'is_znvlp', 'is_narcotic', 
        'is_dosed', 'mass_volume_num', 'mass_volume_name', # 08.11.2022
        'manufacturer_name', 'manufacturer_country_code', 'manufacturer_country_name', 'manufacturer_address', # 15.11.2022
        'proc_tag'
    ]

    # debug, debug_print, b, e = False, True, 0, 100
    # Wall time: 1min 8s
    debug, debug_print, b, e = False, False, 0, zvnlp_df.shape[0]+1

    offset = datetime.timezone(datetime.timedelta(hours=3))
    mask_code_klp_isnull = zvnlp_df['code_klp'].isnull()
    # print(f"start: {datetime.datetime.now(offset).strftime('%Y_%m_%d %H:%M:%S')}")
    logger.info('P2: lp_no_date - Унификация только по штрихкоду - start...')

    # zvnlp_df_mask = zvnlp_df[mask_code_klp_isnull]
    zvnlp_df.loc[mask_code_klp_isnull, new_cols] = zvnlp_df.loc[mask_code_klp_isnull, cols_for_parsing].progress_apply(lambda x: pd.Series(\
            extend_znvlp(x[cols_for_parsing[0]], x[cols_for_parsing[1]], x[cols_for_parsing[2]], x[cols_for_parsing[3]], x[cols_for_parsing[4]],\
                        proc_tag='lp_no_date', is_reg_price=False, debug=debug, debug_print=debug_print),\
            index=new_cols),axis=1) #
    logger.info('P2: lp_no_date - Унификация только по штрихкоду - done!')
    logger.info("Обработано записей (lp_no_date): " + str(zvnlp_df[zvnlp_df['proc_tag']=='lp_no_date'].shape[0]) )

def count_code_klp(code_klp):
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date
    # if code_klp is None or (not type(code_klp)==str) and not ((type(code_klp)==list) or (type(code_klp)==np.ndarray)) and math.isnan(code_klp): return 0, None
    if code_klp is None or ( ((type(code_klp)==float) or (type(code_klp)==np.float64)) and math.isnan(code_klp)): # or\
        # (not (type(code_klp)==str) and not ((type(code_klp)==list) or (type(code_klp)==np.ndarray)) : 
        return 0, None
    elif (type(code_klp) ==str) or (type(code_klp)==np.str_): 
            code_klp_lst = code_klp.split('\n')
            code_klp_lst = [re.sub(r"[\[\]]", "", code_klp) for code_klp in code_klp_lst]
            code_klp_lst_len = len(code_klp_lst)
            if code_klp_lst_len==1: return 1, code_klp_lst[0]
            elif code_klp_lst_len>1: return code_klp_lst_len, code_klp_lst
    elif (type(code_klp)==list) or  (type(code_klp)==np.ndarray):
    # elif not (type(row['code_klp'])==str)  and ((type(row['code_klp'])==list) or  (type(row['code_klp'])==np.ndarray)):
        return len(code_klp), code_klp
            
    # elif type(code_klp) ==list or type(code_klp)==np.ndarray: return len(code_klp)
def apply_p3a_cnt_code_klp():
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date
    logger.info("P3a: Предобработка: Подчсчет кол-ва кодов КЛП...")
    zvnlp_df[['n_code_klp', 'code_klp_upd']] = zvnlp_df['code_klp'].progress_apply(lambda x: pd.Series(count_code_klp(x)))

def update_znvlp_cols_by_one_col(srch_col_name, i_row, row, update_cols_names, cnt, proc_tag2='ph_form, doze, pack',
        # mask_srch_col_name_notnull=mask_srch_col_name_notnull, mask_code_klp_notnull=mask_code_klp_notnull,
        mask_srch_col_name_notnull=None, mask_code_klp_notnull=None,
        debug=False):
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date
    srch_value = row[srch_col_name] 
    mask_srch_value = mask_srch_col_name_notnull & mask_code_klp_notnull \
        & (zvnlp_df[srch_col_name]==srch_value) & (zvnlp_df['n_code_klp']>0)  # 730 records
        # & (zvnlp_df[srch_col_name]==srch_value) & (zvnlp_df['n_code_klp']==1)  # 730 records
        # & (zvnlp_df[srch_col_name]==srch_value) & (zvnlp_df['n_code_klp']>1)  #  116 records
    if debug: print("update_znvlp_cols_by_one_col: zvnlp_df[mask_srch_value].shape[0]", zvnlp_df[mask_srch_value].shape[0])
    if zvnlp_df[mask_srch_value].shape[0]>0:
        if debug: display(zvnlp_df[mask_srch_value].head(1))
        # display(zvnlp_df[mask_srch_value][update_cols_names].head(1))
        if debug: display(pd.DataFrame([row]))
        cnt += 1
        for ii, rrow in zvnlp_df[mask_srch_value].iterrows():
            # zvnlp_df[mask_code_klp_isnull].loc[i, update_cols_names] = zvnlp_df[mask_srch_value].loc[ii, update_cols_names].apply(lambda x: pd.Series(x)) #.values[0]
            # zvnlp_df[mask_code_klp_isnull].loc[i, update_cols_names] = pd.Series(rrow[update_cols_names].values)
            # zvnlp_df[mask_code_klp_isnull].loc[i, update_cols_names] = rrow[update_cols_names].values
            zvnlp_df.loc[i_row, update_cols_names] = rrow[update_cols_names].values
            zvnlp_df.loc[i_row,'proc_tag'] = 'not_matched'
            if row['proc_tag2'] is not None and proc_tag2 is not None and not (proc_tag2 in row['proc_tag2']):
                zvnlp_df.loc[i_row,'proc_tag2'] = row['proc_tag2'] + '+' + proc_tag2
            else: zvnlp_df.loc[i_row,'proc_tag2'] = proc_tag2
            break
        # row_upd = zvnlp_df[mask_code_klp_isnull].loc[i_row, zvnlp_df.columns]
        row_upd = zvnlp_df.loc[i_row, zvnlp_df.columns]
        if debug: 
            print("update_znvlp_cols_by_one_col: row_upd:")
            display(pd.DataFrame([row_upd]))
    return cnt    

def apply_p3a_ph_form():
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date
    srch_col_name = 'Лекарственная форма, дозировка, упаковка (полная)'
    mask_code_klp_isnull = zvnlp_df['code_klp'].isnull()
    mask_code_klp_notnull = zvnlp_df['code_klp'].notnull()
    mask_srch_col_name_notnull = zvnlp_df[srch_col_name].notnull()
    update_cols_names = ['form_standard', 'form_standard_unify', 'doze_group',
       'dosage_standard_value_str', 'dosage_standard_value',
       'dosage_standard_unit', 'pack_1_num', 'pack_1_name', 'pack_2_num',
       'pack_2_name', 'consumer_total', 'ls_unit_okei_name', 'ls_unit_name']
    # debug, b,e, cnt, max_cnt = False, 0, 40000, 0, 40000
    debug, b,e, cnt, max_cnt = False, 0, zvnlp_df.shape[0]+1, 0, zvnlp_df.shape[0]+1
    # Wall time: 32 s w 08.11.2022
    # debug, b,e, cnt, max_cnt = True, 0, 40000, 0, 10
    
    logger.info("P3a:  Частичная унификация по ph_form, doze, pack - start...")
    zvnlp_df['proc_tag2'] = None
    for i, row in tqdm(zvnlp_df[mask_code_klp_isnull].iterrows(), total = zvnlp_df[mask_code_klp_isnull].shape[0]):
        if i < b: continue
        if i > e: break
        if cnt> max_cnt: break
        cnt = update_znvlp_cols_by_one_col(srch_col_name, i, row, update_cols_names, cnt, proc_tag2='ph_form, doze, pack', 
            mask_srch_col_name_notnull=mask_srch_col_name_notnull, mask_code_klp_notnull=mask_code_klp_notnull, debug=debug)

    logger.info("P3a:  Частичная унификация по ph_form, doze, pack - done!")
    # print(cnt) # 567 # 358 # 357
    logger.info(f"P3a:  Обработано {zvnlp_df[zvnlp_df['proc_tag2']== 'ph_form, doze, pack'].shape[0]} записей")

def apply_p3b_pre_clean_tn():
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date
    logger.info("P3b: Предобработка: Очистка ТН от '®' ...")
    zvnlp_df['TN_znvlp_clean'] = zvnlp_df['Торговое наименование лекарственного препарата'].\
        progress_apply(lambda x: x.replace('®','') if (x is not None and not((type(x)==float) or (type(x)==np.float64))) else x)

def apply_p3b_tn_mnn():
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date
    srch_col_name = 'Торговое наименование лекарственного препарата'
    srch_col_name = 'TN_znvlp_clean'
    mask_code_klp_isnull = zvnlp_df['code_klp'].isnull()
    mask_code_klp_notnull = zvnlp_df['code_klp'].notnull()
    mask_srch_col_name_notnull = zvnlp_df[srch_col_name].notnull()
    mask_srch_col_name_isnull = zvnlp_df[srch_col_name].isnull()
    update_cols_names = ['mnn_standard', 'trade_name',
    ]
    mask_update_cols_names_isnull = zvnlp_df['mnn_standard'].isnull() & zvnlp_df['trade_name'].isnull()

    debug, b,e, cnt, max_cnt = False, 0, zvnlp_df.shape[0]+1, 0, zvnlp_df.shape[0]+1
    # debug, b,e, cnt, max_cnt = True, 0, zvnlp_df.shape[0]+1, 0, 0
    # for i, row in zvnlp_df[mask_code_klp_isnull & mask_update_cols_names_isnull].iterrows():
    
    logger.info("P3b:  Частичная унификация по ТН, ИНН - start ...")
    for i, row in tqdm(zvnlp_df[mask_code_klp_isnull & mask_update_cols_names_isnull].iterrows(), \
        total=zvnlp_df[mask_code_klp_isnull & mask_update_cols_names_isnull].shape[0]):
        if i < b: continue
        if i > e: break
        if cnt> max_cnt: break
        cnt = update_znvlp_cols_by_one_col(srch_col_name, i, row, update_cols_names, cnt, proc_tag2='tn_mnn', 
            mask_srch_col_name_notnull=mask_srch_col_name_notnull, mask_code_klp_notnull=mask_code_klp_notnull, debug=debug)

    # print(cnt) # 1060 w 08.11.2022 #1016 # 950
    logger.info("P3b:  Частичная унификация по ТН, ИНН - done!")

    # print('tn_mnn', zvnlp_df[(zvnlp_df['proc_tag']=='not_matched') & (zvnlp_df['proc_tag2']=='tn_mnn')].shape[0]) # tn_mnn 736 # 357
    
    logger.info(f"P3b:  Обработано: 'tn_mnn':  {zvnlp_df[zvnlp_df['proc_tag2'].notnull() & zvnlp_df['proc_tag2']== 'tn_mnn'].shape[0]} записей")    
    logger.info(f"P3b:  Обработано: '+tn_mnn': " +\
        f"{zvnlp_df[zvnlp_df['proc_tag2'].notnull() & zvnlp_df['proc_tag2'].str.contains(re.escape('+tn_mnn'), regex=True)].shape[0]} записей")

def update_znvlp_cols_by_any_cols(srch_col_names_lst, i_row, row, update_cols_names, cnt, proc_tag2='ath',
        # mask_srch_cols_names_notnull=None, 
        mask_code_klp_notnull=None, 
        mask_iterate = None,
        debug=False):
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date        
    # по условию 'И' (&)
    # и только для строковых колонок или (dtype=object)
    if srch_col_names_lst is None or not (type(srch_col_names_lst)==list) or (len(srch_col_names_lst)==0):
        return cnt
    num_cols = len(srch_col_names_lst)
    # srch_value = row[srch_col_name] 
    mask_srch_values_pre = None
    for j, col_name in enumerate(srch_col_names_lst):
        if debug: 
            print(f"update_znvlp_cols_by_any_cols: j: {j}, srch_col_names_lst[j]: {srch_col_names_lst[j]}, type(row[srch_col_names_lst[j]]): {type(row[srch_col_names_lst[j]])}")
            print(f"update_znvlp_cols_by_any_cols: row[srch_col_names_lst[j]]: {row[srch_col_names_lst[j]]}")

        if j ==0: 
            # mask_srch_values_pre = zvnlp_df[srch_col_names_lst[j]].notnull() & (zvnlp_df[srch_col_names_lst[j]]==row[srch_col_names_lst[j]])  
            # не работает без предварительного сохранения в Excel
            # mask_srch_values_pre = zvnlp_df[srch_col_names_lst[j]].notnull() & (zvnlp_df[srch_col_names_lst[j]]==row[srch_col_names_lst[j]][0])  
            mask_srch_values_pre = zvnlp_df[srch_col_names_lst[j]].notnull() & (zvnlp_df[srch_col_names_lst[j]].str.contains(re.escape(row[srch_col_names_lst[j]]) + r"$",regex=True))  
            # zvnlp_df[ zvnlp_df[srch_col_name1].notnull() & zvnlp_df[srch_col_name1].str.contains(re.escape(srch_value1)+r"$",regex=True) & \
        #  zvnlp_df[srch_col_name2].notnull() & zvnlp_df[srch_col_name2].str.contains(re.escape(srch_value2)+r"$",regex=True)]
        else:
            # mask_srch_values_pre = mask_srch_values_pre & zvnlp_df[srch_col_names_lst[j]].notnull() & (zvnlp_df[srch_col_names_lst[j]]== row[srch_col_names_lst[j]]) 
            mask_srch_values_pre = mask_srch_values_pre & zvnlp_df[srch_col_names_lst[j]].notnull() & (zvnlp_df[srch_col_names_lst[j]].str.contains(re.escape(row[srch_col_names_lst[j]]) + r"$",regex=True)) 
        # mask_srch_values = zvnlp_df['mnn_standard'].str.contains(srch_value_01) & zvnlp_df['form_standard'].str.contains(srch_value_02)
    if mask_srch_values_pre is not None:
        mask_srch_values = mask_srch_values_pre & mask_code_klp_notnull & (zvnlp_df['n_code_klp']>0)  # ... records
        # & (zvnlp_df[srch_col_name]==srch_value) & (zvnlp_df['n_code_klp']==1)  #  records
        # & (zvnlp_df[srch_col_name]==srch_value) & (zvnlp_df['n_code_klp']>1)  #   records
        if debug: print("update_znvlp_cols_by_any_cols: zvnlp_df[mask_srch_values].shape[0]", zvnlp_df[mask_srch_values].shape[0])
        if zvnlp_df[mask_srch_values].shape[0]>0:
            if debug: display(zvnlp_df[mask_srch_values].head(1))
            # display(zvnlp_df[mask_srch_value][update_cols_names].head(1))
            if debug: display(pd.DataFrame([row]))
            cnt += 1
            for ii, rrow in zvnlp_df[mask_srch_values].iterrows():
                # zvnlp_df[mask_code_klp_isnull].loc[i, update_cols_names] = zvnlp_df[mask_srch_value].loc[ii, update_cols_names].apply(lambda x: pd.Series(x)) #.values[0]
                # zvnlp_df[mask_code_klp_isnull].loc[i, update_cols_names] = pd.Series(rrow[update_cols_names].values)
                # zvnlp_df[mask_code_klp_isnull].loc[i, update_cols_names] = rrow[update_cols_names].values
                zvnlp_df.loc[i_row, update_cols_names] = rrow[update_cols_names].values
                zvnlp_df.loc[i_row,'proc_tag'] = 'not_matched'
                if row['proc_tag2'] is not None and not (proc_tag2 in row['proc_tag2']):
                    zvnlp_df.loc[i_row,'proc_tag2'] = row['proc_tag2'] + '+' + proc_tag2
                else: zvnlp_df.loc[i_row,'proc_tag2'] = proc_tag2
                break
            row_upd = zvnlp_df[mask_iterate].loc[i_row, zvnlp_df.columns]
            if debug: 
                print("update_znvlp_cols_by_any_cols: row_upd:")
                display(pd.DataFrame([row_upd]))
    return cnt    

def apply_p3c_mnn_standard_form_standard():
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date  
                                
    srch_cols = ['mnn_standard', 'form_standard']
    mask_srch_cols_notnull = zvnlp_df['mnn_standard'].notnull() & zvnlp_df['form_standard'].notnull() 
    mask_ath_name_isnull = zvnlp_df['ath_name'].isnull()
    mask_ath_name_notnull = zvnlp_df['ath_name'].notnull()

    mask_code_klp_isnull = zvnlp_df['code_klp'].isnull()
    mask_iterate = mask_srch_cols_notnull &  mask_ath_name_isnull 

    mask_code_klp_notnull = zvnlp_df['code_klp'].notnull()
    # mask_srch_col_name_notnull = zvnlp_df[srch_col_name].notnull()
    # mask_srch_col_name_isnull = zvnlp_df[srch_col_name].isnull()

    update_cols_names = ['ath_name', 'ath_code', 'is_znvlp', 'is_narcotic','form_standard_unify', 'doze_group',]

    debug, b,e, cnt, max_cnt = False, 0, zvnlp_df.shape[0]+1, 0, zvnlp_df.shape[0]+1
    # debug, b,e, cnt, max_cnt = True, 0, zvnlp_df.shape[0]+1, 0, 0

    logger.info("P3c:  Частичная унификация по mnn_standard, form_standard - start ...")
    # for i, row in zvnlp_df[mask_code_klp_isnull & mask_iterate].iterrows():
    for i, row in tqdm(zvnlp_df[mask_iterate].iterrows(), total = zvnlp_df[mask_iterate].shape[0]):
        if i < b: continue
        if i > e: break
        if cnt> max_cnt: break
        update_znvlp_cols_by_any_cols(srch_cols, i, row, update_cols_names, cnt, proc_tag2='ath', 
            # mask_srch_cols_names_notnull=mask_srch_cols_notnull, 
            mask_code_klp_notnull=mask_code_klp_notnull,
            mask_iterate = mask_iterate,
            debug=debug)
    logger.info("P3c:  Частичная унификация по mnn_standard, form_standard - done!")
    print(cnt) # ath 197  c клд от 10ю11ю2022 313 w 08.11.2022 v2  276 w 08.11.2022 #254
    logger.info('Обработано записей: ath ' + str(zvnlp_df[(zvnlp_df['proc_tag']=='not_matched') & zvnlp_df['proc_tag2'].notnull() &\
        zvnlp_df['proc_tag2'].str.contains('ath')].shape[0]))# 

def update_vol_exclude(vol, vol_unit, mass_volume_name, mass_volume_num, debug=False):
    # vol, vol_unit = None, None
    # есть еще ошибочная ситуация mass_volume_name [14.000, 5.000]	mass_volume_num [кг, литр]
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date  
    if mass_volume_name is not None and not (((type(mass_volume_name)==float) or (type(mass_volume_name)==np.float64)) and math.isnan(mass_volume_name)):
        if ((type(mass_volume_name)==str) or (type(mass_volume_name)==np.str_)) and  mass_volume_name in ["кг", "литр"]:
            if debug: 
                print('if ((type(mass_volume_name)==str) or (type(mass_volume_name)==np.str_)) and  mass_volume_name in ["кг", "литр"]:')
                print(f"vol: {vol}, mass_volume_num: {mass_volume_num}, new_vol: {float(mass_volume_num)*1000}")
            # vol, vol_unit = float(mass_volume_num)*1000, "мл"
            vol, vol_unit = float(float(mass_volume_num)*1000), "мл"
            # try:
            #     # vol, vol_unit = mass_volume_num*1000, "мл"
            #     vol, vol_unit = float(mass_volume_num)*1000, "мл"
            #     # new_vol, vol_unit = float(mass_volume_num)*1000, "мл"
            # except Exception as err:
            #     print(err)
            #     print(f"mass_volume_name: {mass_volume_name}, mass_volume_num: {mass_volume_num}")

        elif ((type(mass_volume_name)==list) or (type(mass_volume_name)==np.ndarray)) and\
            ('кг' in mass_volume_name or  "литр" in mass_volume_name):
            if debug: print('elif ((type(mass_volume_name)==list) or (type(mass_volume_name)==np.ndarray)) and("кг" in mass_volume_name or  "литр" in mass_volume_name):')
            # vol, vol_unit = float(mass_volume_num)*1000, "мл"
            vol, vol_unit = float(float(mass_volume_num)*1000), "мл"
            # try:
            #     # vol, vol_unit = mass_volume_num*1000, "мл"
            #     vol, vol_unit = float(mass_volume_num)*1000, "мл"
            #     # new_vol, vol_unit = float(mass_volume_num)*1000, "мл"
            # except Exception as err:
            #     print(err)
            #     print(f"mass_volume_name: {mass_volume_name}, mass_volume_num: {mass_volume_num}")
            # по алгоритму не правильно считает "литры" "кг" - это косяк ЕСКЛП. 
            # Соот-но прикручивем костыли: если mass_volume_name = "кг", "литр" =>vol*=mass_volume_num*1000, vol_unit* = "мл"
    return vol, vol_unit

def calc_volume(i_row, doze_group, ls_unit_name, pack_1_num, 
                form_standard, consumer_total, consumer_total_znvlp, dosage_standard_unit, mass_volume_name, 
                mass_volume_num,
                cnt, debug=False, write=False):
    # 919 only size-1 arrays can be converted to Python scalars
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date  
    # vol_pre, vol_unit_pre, vol, vol_unit = None, None, None, None
    vol, vol_unit = None, None
    vol_empty, vol_unit_empty = "#НД", "#НД"
    value_ok = '**'
    # update_cols_names = ['vol_pre', 'vol_unit_pre', 'vol', 'vol_unit']
    update_cols_names = ['vol', 'vol_unit']
    if doze_group is None: return cnt
    if doze_group in [0,1,2,4,5,6,7,8,9]:
        if doze_group == 0:
            # vol, vol_unit = vol_empty, vol_unit_empty
            # vol, vol_unit = value_ok, value_ok
            vol, vol_unit = value_ok, vol_unit
        elif doze_group == 1:
            if ls_unit_name == "г лекарственной формы":
                vol, vol_unit = pack_1_num, 'г'
            elif ls_unit_name == "мг действующего вещества":
                # vol, vol_unit = vol_empty, vol_unit_empty + 'мг'
                if form_standard == "ГЕЛЬ ДЛЯ ПОДКОЖНОГО ВВЕДЕНИЯ":
                # ГЕЛЬ ДЛЯ ПОДКОЖНОГО ВВЕДЕНИЯ
                    vol, vol_unit = value_ok, vol_unit
                else:
                    vol, vol_unit = vol_empty, vol_unit_empty
            
            else:
                vol, vol_unit = vol_empty, vol_unit_empty
        elif doze_group == 2:
            if ls_unit_name == "кг":
                vol, vol_unit = pack_1_num, 'кг'
            else:
                vol, vol_unit = vol_empty, vol_unit_empty
        elif doze_group == 4:
            if ls_unit_name == "мл":
                vol, vol_unit = pack_1_num, 'мл'
            else:
                vol, vol_unit = vol_empty, vol_unit_empty
        elif doze_group == 5:
            if ls_unit_name == "мл":
                vol, vol_unit = pack_1_num, 'мл'
            elif ls_unit_name == "г лекарственной формы":
                vol, vol_unit = pack_1_num, 'г'
            elif ls_unit_name == "доз(а)":
                vol, vol_unit = pack_1_num, 'доз(а)'
            else:
                vol, vol_unit = vol_empty, vol_unit_empty
        elif doze_group == 6:
            if form_standard == "ПОРОШОК ДЛЯ ИНГАЛЯЦИЙ ДОЗИРОВАННЫЙ":
                # после восстанвления из Excel надо преобразовать строчный тип consumer_total во float, 
                # посокльку считываемый consumer_total_znvlp - float
                if debug: print(f"calc_volume: type(consumer_total): {type(consumer_total)}, type(consumer_total_znvlp): {type(consumer_total_znvlp)}")
                if (type(consumer_total) ==float) and (type(consumer_total_znvlp) ==float):
                    if debug: print(f"calc_volume: (type(consumer_total) ==float) and (type(consumer_total_znvlp) ==float)")
                    if (consumer_total == consumer_total_znvlp):
                        # vol, vol_unit = vol_empty, vol_unit_empty
                        vol, vol_unit = value_ok, vol_unit
                    else:
                        vol, vol_unit = pack_1_num, ls_unit_name
                elif ((type(consumer_total) == str) or (type(consumer_total) == np.str_)) and ((type(consumer_total_znvlp) ==float) or (type(consumer_total_znvlp) ==np.float64)):
                    if debug: print(f"calc_volume: ((type(consumer_total) == str) or (type(consumer_total) == np.str_)) and ((type(consumer_total_znvlp) ==float) or (type(consumer_total_znvlp) ==np.float64))")
                    try:
                        consumer_total = float(consumer_total)
                        if (consumer_total == consumer_total_znvlp):
                            # vol, vol_unit = vol_empty, vol_unit_empty
                            vol, vol_unit = value_ok, vol_unit
                        else:
                            vol, vol_unit = pack_1_num, ls_unit_name
                    except Exception as err:
                        print(f"calc_volume: 'consumer_total = float(consumer_total) error': {i_row}", err)
                        vol, vol_unit = vol_empty + '#ERR', vol_unit_empty + '#ERR'

            elif dosage_standard_unit in ["мл/доза", "мг/доза", "МЕ/доза"]:
                vol, vol_unit = pack_1_num, ls_unit_name
            else:
                # vol, vol_unit = vol_empty, vol_unit_empty
                vol, vol_unit = value_ok, vol_unit
        elif doze_group == 7:
            if ls_unit_name == "мл":
                vol, vol_unit = pack_1_num, 'мл'
            elif ls_unit_name == "г лекарственной формы":
                vol, vol_unit = pack_1_num, 'г'
            elif ls_unit_name == "доз(а)":
                vol, vol_unit = pack_1_num, 'доз(а)'
            else:
                vol, vol_unit = vol_empty, vol_unit_empty
        elif doze_group == 8:
            # if ls_unit_name == "г лекарственной формы":
            #     vol, vol_unit = pack_1_num, 'г'
            # elif ls_unit_name == "г действующего вещества":
            #     vol, vol_unit = pack_1_num, 'г'
            # else:
            #     vol, vol_unit = vol_empty, vol_unit_empty
            if ls_unit_name in ["г лекарственной формы", "г действующего вещества"]:
                # если "dosage_standard_unit" = ЕД/г, мг/г
                if dosage_standard_unit in ['ЕД/г', 'мг/г']:
                    vol, vol_unit = pack_1_num, 'г'
                else:
                    vol, vol_unit = value_ok, value_ok
            else:
                vol, vol_unit = vol_empty, vol_unit_empty

        elif doze_group == 9:
            # vol, vol_unit = vol_empty, vol_unit_empty
            vol, vol_unit = value_ok, vol_unit
        
        cnt += 1
    
        # if doze_group in [0,1,2,4,5,6,7,8,9]
        vol, vol_unit = update_vol_exclude(vol, vol_unit, mass_volume_name, mass_volume_num, debug)
        if debug: print(f"calc_volume: doze_group: {doze_group}, vol: {vol}, vol_unit: {vol_unit}")    
            # if debug: print(f"calc_volume: doze_group: {doze_group}, vol: {vol}, vol_unit: {vol_unit}")
    
    # zvnlp_df[mask_doze_group_notnull].loc[i_row, update_cols_names] = pd.Series([vol, vol_unit])
    if write:
        # zvnlp_df.loc[i_row, update_cols_names] = [vol, vol_unit]
        zvnlp_df.loc[i_row, update_cols_names] = np.array([vol, vol_unit], dtype=object)
        # zvnlp_df.loc[i_row, update_cols_names] = vol, vol_unit
        # if debug: 
        #     row_upd = zvnlp_df[mask_doze_group_notnull].loc[i_row, zvnlp_df.columns]
        #     print("calc_volume: row_upd:")
        #     display(pd.DataFrame([row_upd]))


    return cnt

def apply_p4_calc_volumes():
    # 919 only size-1 arrays can be converted to Python scalars ZNNVLP 19.102.2022 ESKLP 23.09.2022
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date  
                                # mask_srch_col_name_notnull=mask_srch_col_name_notnull, mask_code_klp_notnull=mask_code_klp_notnull,
        # mask_iterate = mask_iterate,
    mask_doze_group_notnull = zvnlp_df['doze_group'].notnull()
    # update_cols_names = ['vol_pre', 'vol_unit_pre', 'vol', 'vol_unit']
    update_cols_names = ['vol', 'vol_unit']

    debug, write, b,e, cnt, max_cnt = False, True, 0, zvnlp_df.shape[0]+1, 0, zvnlp_df.shape[0]+1
    # Wall time: 1min 18s
    # debug, b,e, cnt, max_cnt = True, 0, 1000, 0, 10
    # Wall time: 267 ms
    # debug, write, b,e, cnt, max_cnt = True, False, 22, 25, 22, 25

    logger.info("P4: Расчет объемов - start ...")
    zvnlp_df[update_cols_names] = None
    # zvnlp_df[update_cols_names] = zvnlp_df[update_cols_names].astype(object)
    for i, row in tqdm(zvnlp_df[mask_doze_group_notnull].iterrows(), total = zvnlp_df[mask_doze_group_notnull].shape[0]):
    # for i, row in zvnlp_df[mask_code_klp_notnull & (zvnlp_df['n_code_klp']>1)].iterrows():  
        if i < b: continue
        if i > e: break
        if cnt> max_cnt: break
        doze_group, ls_unit_name, pack_1_num, form_standard, consumer_total, consumer_total_znvlp, \
            dosage_standard_unit, mass_volume_name, mass_volume_num = \
            row['doze_group'], row['ls_unit_name'], row['pack_1_num'], row['form_standard'], \
            row['consumer_total'], row['Коли-\nчество в потреб. упаков-\nке'], \
            row['dosage_standard_unit'], row['mass_volume_name'], row['mass_volume_num']
        try:
            cnt = calc_volume(i, doze_group, ls_unit_name, pack_1_num, 
                        form_standard, consumer_total, consumer_total_znvlp, 
                        dosage_standard_unit, mass_volume_name, mass_volume_num,

    
                        cnt, debug=debug, write=write)
        except Exception as err:
            print(i, err)
    #     \Program Files\Python310\lib\site-packages\numpy\core\fromnumeric.py:3199: VisibleDeprecationWarning: Creating an ndarray from ragged nested sequences (which is a list-or-tuple of lists-or-tuples-or ndarrays with different lengths or shapes) is deprecated. If you meant to do this, you must specify 'dtype=object' when creating the ndarray.
    #   return asarray(a).ndim
    #  91
    logger.info("P4: Расчет объемов - done!")
    logger.info(f"Обработано записей: {cnt}")

def define_main_units(cmplx_doze_units_lst, debug=False):
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date
    # last_unit = cmplx_doze_units_lst[-1].replace('/ ','/')
    fst_unit, last_unit, unit_types_num, fst_unit_is_part_of_last_unit = None, None, 0, None
    if (type(cmplx_doze_units_lst)==list) & (type(cmplx_doze_units_lst[0])==str):
        last_unit = re.sub(r'/ *', '/', cmplx_doze_units_lst[-1]).strip()
        fst_unit = cmplx_doze_units_lst[0].strip()
        
        if debug: print(f"proc_complex_doze: fst_unit: '{fst_unit}', last_unit: '{last_unit}'")
        if len(set(cmplx_doze_units_lst))==1: # все значения совпадают
            unit_types_num = 1
            fst_unit_is_part_of_last_unit = True
        else: # несколько занчений
            fst_units = list(set(cmplx_doze_units_lst[:-1]))
            if len (fst_units)==1: # все кроме последнего занчения units - совпадают 
                unit_types_num = 2
                # (если только два еще проверка на подмножество)
                # if fst_units[0].issubset(last_unit): # мг часть мг/г
                if last_unit.find(fst_units[0])>-1: # мг часть мг/г
                    fst_unit_is_part_of_last_unit = True
                else:
                    fst_unit_is_part_of_last_unit = False
            else:
                unit_types_num = 1 + len (fst_units)
                fst_unit_is_part_of_last_unit = None
            
    return fst_unit, last_unit, unit_types_num, fst_unit_is_part_of_last_unit

def proc_complex_doze(i_row, doze_group, dosage_standard_value_str,
                      cnt, debug=False, write=True):
    proc_tag3 = None
    proc_tag3_value = 'cmplx_doze'
    dosage_standard_value, dosage_standard_unit = None, None
    # vol, vol_unit = None, None
    # vol_empty, vol_unit_empty = "#НД", "#НД"
    value_empty = "#НД"
    dosage_standard_value_empty, dosage_standard_unit_empty = "#НД", "#НД"
    # update_cols_names = ['dosage_standard_value', 'dosage_standard_unit', 'vol', 'vol_unit', 'proc_tag3']
    update_cols_names = ['dosage_standard_value', 'dosage_standard_unit', 'pseudo_vol', 'proc_tag3']
    fl_pseudo_vol = False
    pseudo_vol = None
    if doze_group is None or dosage_standard_value_str is None \
        or (type(dosage_standard_value_str)==float and math.isnan(dosage_standard_value_str)) \
        or re.search(r"(\d+\.*-\d*)", dosage_standard_value_str) is not None \
        or (dosage_standard_value_str.lower()=='не указано') \
        or  (', ' in dosage_standard_value_str):
        # return cnt
        # dosage_standard_value, dosage_standard_unit = None, None
        dosage_standard_value, dosage_standard_unit, pseudo_vol = None, None, None
        pass
    elif dosage_standard_value_str in ["~"]: # 'не указано'
        # dosage_standard_value, dosage_standard_unit = dosage_standard_value_empty, "~"
        # если dosage_standard_value_str = ~ => dosage_standard_unit = ~ (это сделано), dosage_standard_value = ~
        # dosage_standard_value, dosage_standard_unit = "~", "~"
        dosage_standard_value, dosage_standard_unit, pseudo_vol = "~", "~", None
        proc_tag3 = proc_tag3_value
    elif '+' in dosage_standard_value_str: # ксть еще минус "8-15 млн/мг"
        cmplx_doze_lst = dosage_standard_value_str.split('+')
        if len(cmplx_doze_lst)==1: # все-таки нет плюса
            # return cnt
            # dosage_standard_value, dosage_standard_unit = None, None
            dosage_standard_value, dosage_standard_unit, pseudo_vol = None, None, None
            pass
        elif len(cmplx_doze_lst)>1:
            cnt += 1
            proc_tag3 = proc_tag3_value
            cmplx_doze_lst = [d.strip() for d in cmplx_doze_lst]
            # if re.search(r"/\s*\d+\.*\d*" , cmplx_doze_lst[-1]) is not None: # есть цифра псевдообъема 50 мг/5 мл
            #     fl_pseudo_vol = True
            # fl_pseudo_vol = False
            for d in cmplx_doze_lst:
                if re.search(r"/\s*\d+\.*\d*" , d) is not None: # есть цифра псевдообъема 50 мг/5 мл
                    fl_pseudo_vol = True
                    break
            #     b_pos = cmplx_doze_lst[-1].rfind('/')
            #     cmplx_doze_lst[-1] = cmplx_doze_lst[-1][:b_pos+1] + re.sub(r"[(\d*\.\d*)]", '', cmplx_doze_lst[-1][b_pos+1:]).strip()
            if debug: 
                print("proc_complex_doze: dosage_standard_value_str-->:", dosage_standard_value_str,
                            f"cmplx_doze_lst: {cmplx_doze_lst}")
            
            if fl_pseudo_vol:
                # cmplx_doze_values_lst = [float(re.sub (r"[^(\d*\.\d*)]", '', re.sub(r"(/.*)", '',d))) for d in cmplx_doze_lst]
                cmplx_doze_values_lst = [
                        float(re.sub(re.sub (r"(\d*\.*\d)|(\d\.*\d*)", '', re.sub(r"(/.*)", '',d)).strip(), '', re.sub(r"(/.*)", '',d)).strip()) 
                            for d in cmplx_doze_lst]
                


                # cmplx_doze_units_lst = [re.sub(r"[(\d*\.\d*)]", '', d).strip() for d in cmplx_doze_lst[:-1]]+\
                #   [re.sub(r"[(\d*\.\d*)]", '',cmplx_doze_lst[-1][:cmplx_doze_lst[-1].rfind('/')]).strip()+ \
                #   cmplx_doze_lst[-1][cmplx_doze_lst[-1].rfind('/'):].strip()]
                cmplx_doze_units_lst = [re.sub(r"(\d*\.*\d)|(\d\.*\d*)", '', d).strip() for d in cmplx_doze_lst[:-1]]+\
                          [re.sub(r"(\d*\.*\d)|(\d\.*\d*)", '',cmplx_doze_lst[-1][:cmplx_doze_lst[-1].rfind('/')]).strip()+ \
                          cmplx_doze_lst[-1][cmplx_doze_lst[-1].rfind('/'):].strip()]
            else:
                try:
                    # 24/08/2023
                    # cmplx_doze_lst: '['2 МЕ', '5.5 мг/кв.см']' 
                    # re.search(r"/\s*\d+\.*\d*" , d)
                    # 2 МЕ+5.5 мг/кв.см
                    # [12:22:07] [ERROR] > error element: '5.5 мг/кв.см'
                    # [12:22:07] [ERROR] > could not convert string to float: '5.5.'
                    # cmplx_doze_values_lst = [float(re.sub (r"[^(\d*\.\d*)]", '', d)) for d in cmplx_doze_lst]
                    cmplx_doze_values_lst = [float(re.sub(re.sub (r"(\d*\.*\d)|(\d\.*\d*)", '', d).strip(), '', d).strip()) for d in cmplx_doze_lst]
                    # print(re.sub (r"(\d*\.\d)|(\d\.\d*)", '', d).strip())
                    # print(re.sub(re.sub (r"(\d*\.\d)|(\d\.\d*)", '', d).strip(), '', d).strip())
                except Exception as err:
                    logger.error(str(err))
                    logger.error(f"cmplx_doze_lst: '{str(cmplx_doze_lst)}'")
                    cmplx_doze_values_lst = []
                    for d in cmplx_doze_lst:
                        try:
                            cmplx_doze_values_lst.append(float(re.sub (r"(\d*\.*\d)|(\d\.*\d*)", '', d)))
                        except: 
                            logger.error(f"error element: '{str(d)}'")
                # cmplx_doze_units_lst = [re.sub(r"[(\d*\.\d*)]", '', d).strip() for d in cmplx_doze_lst]
                
                cmplx_doze_units_lst = [re.sub(r"(\d*\.*\d)|(\d\.*\d*)", '', d).strip() for d in cmplx_doze_lst]
                
            # cmplx_doze_units_lst = [re.sub(r"(?<!/)[(\d*\.\d*)]", '', d).strip() for d in cmplx_doze_lst]
            # cmplx_doze_units_lst = [re.sub(r"(?<!\/)[(\d*\.\d*)]", '', d).strip() for d in cmplx_doze_lst]
            if debug: 
                print("proc_complex_doze: dosage_standard_value_str-->:", dosage_standard_value_str,
                            f"cmplx_doze_values_lst: {cmplx_doze_values_lst}, cmplx_doze_units_lst: {cmplx_doze_units_lst}")
            fst_unit, last_unit, unit_types_num, fst_unit_is_part_of_last_unit = define_main_units(cmplx_doze_units_lst)
            if unit_types_num > 2: # Не обрабатываем
                # dosage_standard_value, dosage_standard_unit = dosage_standard_value_empty, dosage_standard_unit_empty
                dosage_standard_value, dosage_standard_unit, pseudo_vol = value_empty, value_empty, None
            # elif unit_types_num==1: # все значения совпадают
            #     dosage_standard_unit = last_unit
            # несколько занчений
            elif doze_group in [0,1,2,4,5,6,7,8,9]:
                if doze_group == 0:
                    if unit_types_num==1 and last_unit in ['мг']:
                        dosage_standard_value, dosage_standard_unit, pseudo_vol = sum(cmplx_doze_values_lst), 'мг', 1.0
                    elif unit_types_num==2 and fst_unit in ['мг'] and last_unit in ['мг/доза']:
                        # dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг'
                        dosage_standard_value, dosage_standard_unit, pseudo_vol = sum(cmplx_doze_values_lst), 'мг/доза', 1.0
                    else:
                        # dosage_standard_value, dosage_standard_unit = dosage_standard_value_empty, dosage_standard_unit_empty
                        dosage_standard_value, dosage_standard_unit, pseudo_vol = value_empty, value_empty, None

                elif doze_group == 1:
                    if unit_types_num==2 and fst_unit in ['мг'] and last_unit in ['мг/г']:
                        # dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг/г'
                        dosage_standard_value, dosage_standard_unit, pseudo_vol = sum(cmplx_doze_values_lst), 'мг/г', 1.0
                    else:
                        # dosage_standard_value, dosage_standard_unit = dosage_standard_value_empty, dosage_standard_unit_empty
                        dosage_standard_value, dosage_standard_unit, pseudo_vol = dosage_standard_value_empty, dosage_standard_unit_empty, 1.0
                # elif doze_group == 2:
                # elif doze_group == 4:
                elif doze_group == 5:
                    if unit_types_num==2 and fst_unit in ['мг'] and last_unit in ['мг/мл']:
                        # dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг/мл'
                        dosage_standard_value, dosage_standard_unit, pseudo_vol = sum(cmplx_doze_values_lst), 'мг/мл', 1.0
                    elif unit_types_num==2 and fst_unit in ['мг'] and last_unit in ['мг/доза']:
                        # dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг/доза'
                        dosage_standard_value, dosage_standard_unit, pseudo_vol = sum(cmplx_doze_values_lst), 'мг/доза', 1.0
                    else:
                        # dosage_standard_value, dosage_standard_unit = dosage_standard_value_empty, dosage_standard_unit_empty
                        dosage_standard_value, dosage_standard_unit, pseudo_vol = value_empty, value_empty, None
                elif doze_group == 6:
                    if unit_types_num==1 and last_unit in ['мг']:
                        # dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг'
                        dosage_standard_value, dosage_standard_unit, pseudo_vol = sum(cmplx_doze_values_lst), 'мг', 1.0

                    # elif unit_types_num==2 and fst_unit in ['мг'] and last_unit in ['МЕ']:
                    #     dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'МЕ'
                    elif unit_types_num==1 and last_unit in ['МЕ']:
                        # dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'МЕ'
                        dosage_standard_value, dosage_standard_unit, pseudo_vol = sum(cmplx_doze_values_lst), 'МЕ', 1.0
                    elif unit_types_num==2 and fst_unit in ['мг'] and last_unit in ['мг/доза']:
                        # dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг/доза'
                        dosage_standard_value, dosage_standard_unit, pseudo_vol = sum(cmplx_doze_values_lst), 'мг/доза', 1.0
                    elif unit_types_num==2 and fst_unit in ['мг'] and not (fl_pseudo_vol) and last_unit in ['мг/мл']:
                        # dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг'
                        # dosage_standard_value, dosage_standard_unit, pseudo_vol = sum(cmplx_doze_values_lst), 'мг', 1.0
                        dosage_standard_value, dosage_standard_unit, pseudo_vol = sum(cmplx_doze_values_lst), 'мг/мл', 1.0
                    elif unit_types_num==2 and fst_unit in ['мг'] and fl_pseudo_vol and 'мг/' in last_unit and 'мл' in last_unit:
                        # dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг'
                        dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг/мл'
                        ### уточнить
                        # pseudo_vol  = cmplx_doze_lst[-1][cmplx_doze_lst[-1].rfind('/'):].strip()
                        # print(f"last_unit: {last_unit}")
                        pseudo_vol_str = re.sub(r"[^\d\.\,]", '', last_unit[last_unit.rfind('/')+1:].strip())
                        try: 
                            pseudo_vol = float(pseudo_vol_str)
                        except:
                            pseudo_vol = pseudo_vol_str
                    else:
                        # dosage_standard_value, dosage_standard_unit = dosage_standard_value_empty, dosage_standard_unit_empty
                        dosage_standard_value, dosage_standard_unit, pseudo_vol = value_empty, value_empty, None
                    if debug:
                        print(f"dosage_standard_value: {dosage_standard_value}, dosage_standard_unit: {dosage_standard_unit}, pseudo_vol: {pseudo_vol}")
                elif doze_group == 7:
                    if unit_types_num==2 and fst_unit in ['мг'] and last_unit in ['мг/доза']:
                        # dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг/доза'
                        dosage_standard_value, dosage_standard_unit, pseudo_vol = sum(cmplx_doze_values_lst), 'мг/доза', 1.0
                    else:
                        # dosage_standard_value, dosage_standard_unit = dosage_standard_value_empty, dosage_standard_unit_empty
                        dosage_standard_value, dosage_standard_unit, pseudo_vol = value_empty, value_empty, None
                # elif doze_group == 8:
                # elif doze_group == 9:
            
            
            
    if debug: print(f"proc_complex_doze: dosage_standard_value: {dosage_standard_value}, dosage_standard_unit: {dosage_standard_unit}, pseudo_vol: {pseudo_vol}" )
    
    # zvnlp_df[mask_doze_group_notnull].loc[i_row, update_cols_names] = pd.Series([vol, vol_unit])
    if write:
        # zvnlp_df[mask_doze_group_notnull].loc[i_row, update_cols_names] = pd.Series([dosage_standard_value, dosage_standard_unit, proc_tag3])
        # zvnlp_df.loc[i_row, update_cols_names] = pd.Series([dosage_standard_value, dosage_standard_unit, proc_tag3])
        zvnlp_df.loc[i_row, update_cols_names] = dosage_standard_value, dosage_standard_unit, pseudo_vol, proc_tag3
        # zvnlp_df[mask_dosage_standard_value_isnull].loc[i_row, update_cols_names] = dosage_standard_value, dosage_standard_unit, proc_tag3
        if debug: 
            row_upd = zvnlp_df.loc[i_row, zvnlp_df.columns]
            print("proc_complex_doze: row_upd:")
            # display(pd.DataFrame([row_upd]))

    return cnt
def proc_complex_doze_00(i_row, doze_group, dosage_standard_value_str,
                          cnt, debug=False, write=True):
    #     global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date
    #     proc_tag3 = None
    #     proc_tag3_value = 'cmplx_doze'
    #     dosage_standard_value, dosage_standard_unit = None, None
    #     # vol, vol_unit = None, None
    #     # vol_empty, vol_unit_empty = "#НД", "#НД"
    #     dosage_standard_value_empty, dosage_standard_unit_empty = "#НД", "#НД"
    #     # update_cols_names = ['dosage_standard_value', 'dosage_standard_unit', 'vol', 'vol_unit', 'proc_tag3']
    #     update_cols_names = ['dosage_standard_value', 'dosage_standard_unit', 'proc_tag3']
    #     fl_pseudo_vol = False
    #     if doze_group is None or dosage_standard_value_str is None \
    #         or (type(dosage_standard_value_str)==float and math.isnan(dosage_standard_value_str)) \
    #         or re.search(r"(\d+\.*-\d*)", dosage_standard_value_str) is not None \
    #         or (dosage_standard_value_str.lower()=='не указано') \
    #         or  (', ' in dosage_standard_value_str):
    #         # return cnt
    #         dosage_standard_value, dosage_standard_unit = None, None
    #         pass
    #     elif dosage_standard_value_str in ["~"]: # 'не указано'
    #         # dosage_standard_value, dosage_standard_unit = dosage_standard_value_empty, "~"
    #         # если dosage_standard_value_str = ~ => dosage_standard_unit = ~ (это сделано), dosage_standard_value = ~
    #         dosage_standard_value, dosage_standard_unit = "~", "~"
    #         proc_tag3 = proc_tag3_value
    #     elif '+' in dosage_standard_value_str: # ксть еще минус "8-15 млн/мг"
    #         cmplx_doze_lst = dosage_standard_value_str.split('+')
    #         if len(cmplx_doze_lst)==1: # все-таки нет плюса
    #             # return cnt
    #             dosage_standard_value, dosage_standard_unit = None, None
    #             pass
    #         elif len(cmplx_doze_lst)>1:
    #             cnt += 1
    #             proc_tag3 = proc_tag3_value
    #             cmplx_doze_lst = [d.strip() for d in cmplx_doze_lst]
    #             if re.search(r"/\s*\d+\.*\d*" , cmplx_doze_lst[-1]) is not None: # есть цифра псевдообъема 50 мг/5 мл
    #                 fl_pseudo_vol = True
    #             #     b_pos = cmplx_doze_lst[-1].rfind('/')
    #             #     cmplx_doze_lst[-1] = cmplx_doze_lst[-1][:b_pos+1] + re.sub(r"[(\d*\.\d*)]", '', cmplx_doze_lst[-1][b_pos+1:]).strip()
    #             if debug: 
    #                 print("proc_complex_doze: dosage_standard_value_str-->:", dosage_standard_value_str,
    #                             f"cmplx_doze_lst: {cmplx_doze_lst}")
                
    #             if fl_pseudo_vol:
    #                 cmplx_doze_values_lst = [float(re.sub (r"[^(\d*\.\d*)]", '', re.sub(r"(/.*)", '',d))) for d in cmplx_doze_lst]
    #                 cmplx_doze_units_lst = [re.sub(r"[(\d*\.\d*)]", '', d).strip() for d in cmplx_doze_lst[:-1]]+\
    #                   [re.sub(r"[(\d*\.\d*)]", '',cmplx_doze_lst[-1][:cmplx_doze_lst[-1].rfind('/')])+ \
    #                   cmplx_doze_lst[-1][cmplx_doze_lst[-1].rfind('/'):]]
    #             else:
    #                 cmplx_doze_values_lst = [float(re.sub (r"[^(\d*\.\d*)]", '', d)) for d in cmplx_doze_lst]
    #                 cmplx_doze_units_lst = [re.sub(r"[(\d*\.\d*)]", '', d).strip() for d in cmplx_doze_lst]
    #             # cmplx_doze_units_lst = [re.sub(r"(?<!/)[(\d*\.\d*)]", '', d).strip() for d in cmplx_doze_lst]
    #             # cmplx_doze_units_lst = [re.sub(r"(?<!\/)[(\d*\.\d*)]", '', d).strip() for d in cmplx_doze_lst]
    #             if debug: 
    #                 print("proc_complex_doze: dosage_standard_value_str-->:", dosage_standard_value_str,
    #                             f"cmplx_doze_values_lst: {cmplx_doze_values_lst}, cmplx_doze_units_lst: {cmplx_doze_units_lst}")
    #             fst_unit, last_unit, unit_types_num, fst_unit_is_part_of_last_unit = define_main_units(cmplx_doze_units_lst)
    #             if unit_types_num > 2: # Не обрабатываем
    #                 dosage_standard_value, dosage_standard_unit = dosage_standard_value_empty, dosage_standard_unit_empty
    #             # elif unit_types_num==1: # все значения совпадают
    #             #     dosage_standard_unit = last_unit
    #             # несколько занчений
    #             elif doze_group in [0,1,2,4,5,6,7,8,9]:
    #                 if doze_group == 0:
    #                     if unit_types_num==1 and last_unit in ['мг']:
    #                         dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг'
    #                     elif unit_types_num==2 and fst_unit in ['мг'] and last_unit in ['мг/доза']:
    #                         # dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг'
    #                         dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг/доза'

    #                     else:
    #                         dosage_standard_value, dosage_standard_unit = dosage_standard_value_empty, dosage_standard_unit_empty

    #                 elif doze_group == 1:
    #                     if unit_types_num==2 and fst_unit in ['мг'] and last_unit in ['мг/г']:
    #                         dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг/г'
    #                     else:
    #                         dosage_standard_value, dosage_standard_unit = dosage_standard_value_empty, dosage_standard_unit_empty
    #                 # elif doze_group == 2:
    #                 # elif doze_group == 4:
    #                 elif doze_group == 5:
    #                     if unit_types_num==2 and fst_unit in ['мг'] and last_unit in ['мг/мл']:
    #                         dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг/мл'
    #                     elif unit_types_num==2 and fst_unit in ['мг'] and last_unit in ['мг/доза']:
    #                         dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг/доза'
    #                     else:
    #                         dosage_standard_value, dosage_standard_unit = dosage_standard_value_empty, dosage_standard_unit_empty
    #                 elif doze_group == 6:
    #                     if unit_types_num==1 and last_unit in ['мг']:
    #                         dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг'
    #                     # elif unit_types_num==2 and fst_unit in ['мг'] and last_unit in ['МЕ']:
    #                     #     dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'МЕ'
    #                     elif unit_types_num==1 and last_unit in ['МЕ']:
    #                         dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'МЕ'
    #                     elif unit_types_num==2 and fst_unit in ['мг'] and last_unit in ['мг/доза']:
    #                         dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг/доза'
    #                     elif unit_types_num==2 and fst_unit in ['мг'] and not (fl_pseudo_vol) and last_unit in ['мг/мл']:
    #                         dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг'
    #                     elif unit_types_num==2 and fst_unit in ['мг'] and fl_pseudo_vol and 'мг/' in last_unit:
    #                         dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг'
    #                     else:
    #                         dosage_standard_value, dosage_standard_unit = dosage_standard_value_empty, dosage_standard_unit_empty
    #                 elif doze_group == 7:
    #                     if unit_types_num==2 and fst_unit in ['мг'] and last_unit in ['мг/доза']:
    #                         dosage_standard_value, dosage_standard_unit = sum(cmplx_doze_values_lst), 'мг/доза'
    #                     else:
    #                         dosage_standard_value, dosage_standard_unit = dosage_standard_value_empty, dosage_standard_unit_empty
    #                 # elif doze_group == 8:
    #                 # elif doze_group == 9:

        # if debug: print("proc_complex_doze: dosage_standard_value, dosage_standard_unit:", dosage_standard_value, dosage_standard_unit)
        
        # # zvnlp_df[mask_doze_group_notnull].loc[i_row, update_cols_names] = pd.Series([vol, vol_unit])
        # if write:
        #     # zvnlp_df[mask_doze_group_notnull].loc[i_row, update_cols_names] = pd.Series([dosage_standard_value, dosage_standard_unit, proc_tag3])
        #     # zvnlp_df.loc[i_row, update_cols_names] = pd.Series([dosage_standard_value, dosage_standard_unit, proc_tag3])
        #     # zvnlp_df.loc[i_row, update_cols_names] = np.array([dosage_standard_value, dosage_standard_unit, proc_tag3], dtype=object)
        #     zvnlp_df.loc[i_row, update_cols_names] = dosage_standard_value, dosage_standard_unit, proc_tag3
        #     # zvnlp_df[mask_dosage_standard_value_isnull].loc[i_row, update_cols_names] = dosage_standard_value, dosage_standard_unit, proc_tag3
        #     if debug: 
        #         row_upd = zvnlp_df.loc[i_row, zvnlp_df.columns]
        #         print("proc_complex_doze: row_upd:")
        #         display(pd.DataFrame([row_upd]))

        # return cnt
        pass

def apply_p5_calc_complex_doze(debug=False):
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date  
    logger.info("P5: Обработка сложных дозировок - start ...")
    mask_doze_group_notnull = zvnlp_df['doze_group'].notnull()
    mask_dosage_standard_value_isnull = zvnlp_df['dosage_standard_value_str'].notnull() & \
    (zvnlp_df['dosage_standard_value_str'].str.contains(r'\+', regex=True) | zvnlp_df['dosage_standard_value_str'].str.contains('~'))
    # для второго расчета

    # debug, write, b,e, cnt, max_cnt = False, True, 0, zvnlp_df.shape[0]+1, 0, zvnlp_df.shape[0]+1
    write, b,e, cnt, max_cnt = True, 0, zvnlp_df.shape[0]+1, 0, zvnlp_df.shape[0]+1
    # Wall time: 1min 18s
    # debug, write, b,e, cnt, max_cnt = True, True, 0, 3000, 0, 10
    # debug, write, b,e, cnt, max_cnt = False, True, 0, zvnlp_df.shape[0]+1, 0, 20
    # debug, b,e, cnt, max_cnt = True, 0, 1000, 0, 0
    # update_cols_names = ['dosage_standard_value', 'dosage_standard_unit', 'proc_tag3']
    update_cols_names = ['dosage_standard_value', 'dosage_standard_unit', 'pseudo_vol', 'proc_tag3']
    # ### zvnlp_df[update_cols_names] = None
    zvnlp_df['proc_tag3'] = None
    for i, row in tqdm(zvnlp_df[mask_dosage_standard_value_isnull].iterrows(), total = zvnlp_df[mask_dosage_standard_value_isnull].shape[0]):
    # for i, row in zvnlp_df.iterrows():
        # if i < b: continue
        # if i > e: break
        if cnt> max_cnt: break
        doze_group, dosage_standard_value_str = \
            row['doze_group'], row['dosage_standard_value_str']
        cnt = proc_complex_doze(i, doze_group, dosage_standard_value_str, cnt, debug=debug, write=write)  
        # break
    # print(cnt) # 1204 с ЕСКЛП jn 10.11.2022 1239 w 08.11.2022 #1
    logger.info("P5: Обработка сложных дозировок - done!")
    logger.info(f"Обработано записей: {cnt}")

def calc_doze_ls(i_row, vol, vol_unit, dosage_standard_value, dosage_standard_unit,
                      cnt, debug=False, write=True):
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date  
    doze_by_vol, doze_unit_by_vol, doze_by_doze, doze_unit_by_doze, \
          doze_by_doze_vol, doze_unit_by_doze_vol = None, None, None, None, None, None
    value_empty = "#НД"
    value_ok = '**'
    update_cols_names = ['расчет ЛС (по объему)', 'ЕИ ЛС (по объему)',  'расчет ЛС (по дозировке)', 'ЕИ ЛС (по дозировке)', 
                         'расчет ЛС (по дозировке/объему)', 'ЕИ ЛС (по дозировке/объему)']
    cnt += 1
    
    doze_by_vol, doze_unit_by_vol = vol, vol_unit
    
    doze_by_doze = dosage_standard_value
    # if dosage_standard_unit is not None or not ((type(dosage_standard_unit)==float) and  math,isnan(dosage_standard_unit)):
    if dosage_standard_unit is not None and (type(dosage_standard_unit)==str):
        doze_unit_by_doze = re.sub(r"(/.*)", '', dosage_standard_unit).strip()
    else:
        doze_unit_by_doze = dosage_standard_unit
    
    if debug: 
        print(i_row)
        print(f"calc_doze_ls: type(dosage_standard_unit): {type(dosage_standard_unit)}, dosage_standard_unit: {dosage_standard_unit}")
        print(f"calc_doze_ls:  type(dosage_standard_value): {type(dosage_standard_value)}, type(vol): {type(vol)}", 
              f"'type(vol)==np.ndarray': {type(vol)==np.ndarray}")
        print((((type(vol)==list) or (type(vol)==np.ndarray)) and len(vol)>0) or\
            (((type(vol)==str) or (type(vol)==np.str_)) and '[' in vol))

    if vol is not None and not (((type(vol)==float) or (type(vol)==np.float64)) and math.isnan(vol)):
        if (((type(vol)==list) or (type(vol)==np.ndarray)) and len(vol)>0) or\
            (((type(vol)==str) or (type(vol)==np.str_)) and '[' in vol): 
            #     # error vol ['40.000' '50.000']
            doze_by_doze_vol = value_empty
        # elif vol is not None and not (((type(vol)==float) or (type(vol)==np.float64)) and math.isnan(vol)):
        
        elif vol==value_empty:
            doze_by_doze_vol = value_empty
        elif vol==value_ok:
            doze_by_doze_vol = dosage_standard_value
        # elif (((type(vol)==list) or (type(vol)==np.ndarray)) and len(vol)>0) or\
        #     (((type(vol)==str) or (type(vol)==np.str_)) and '[' in vol): 
        #     doze_by_doze_vol = value_empty
        else:
            if ((type(dosage_standard_value) == str) or (type(dosage_standard_value) == np.str_))\
                or ((type(dosage_standard_value) == float) or (type(dosage_standard_value) == np.float64)):
                if dosage_standard_value == value_empty:
                    doze_by_doze_vol = value_empty
                elif dosage_standard_value == '~':
                    doze_by_doze_vol = vol
                    doze_unit_by_doze_vol = vol_unit
                elif ((type(dosage_standard_value) == str) or (type(dosage_standard_value) == np.str_)) and '[' in dosage_standard_value:
                    doze_by_doze_vol = value_empty

                # elif ((type(dosage_standard_value) == list) or (type(dosage_standard_value) == np.ndarray)) and \
                #     len(dosage_standard_value)>1: #>0
                #     doze_by_doze_vol = value_empty
                # перенес ниже наодин таб вправо
                elif (((type(dosage_standard_value)==list) or (type(dosage_standard_value)==np.ndarray)) and len(dosage_standard_value)>0) \
                    or (((type(vol)==list) or (type(vol)==np.ndarray)) and len(vol)>0):
                    doze_by_doze_vol = value_empty

                elif ((type(dosage_standard_unit) == str) or (type(dosage_standard_unit) == np.str_)):
                    if not ('/' in dosage_standard_unit):
                        # если dosage_standard_unit - НЕ содержит "/" => расчет ЛС (по дозировке/объему) = dosage_standard_value
                        doze_by_doze_vol = dosage_standard_value
                    
                    elif ('/' in dosage_standard_unit):
                        #  если то что идет после "/" = vol_unit => расчет ЛС (по дозировке/объему) = dosage_standard_value*vol
                        pseudo_vol_unit = dosage_standard_unit[dosage_standard_unit.rfind('/')+1:].strip()
                        pseudo_vol_unit = re.sub(r"[(\d*\.\d*)]", '', pseudo_vol_unit)
                        if debug: print(f"calc_doze_ls: vol_unit: '{vol_unit}', pseudo_vol_unit: '{pseudo_vol_unit}'")
                        # на всякий случай очищаем от цифр , вдруг где-то осталось
                        if (pseudo_vol_unit == vol_unit) or ((pseudo_vol_unit=='доза') and  (vol_unit =='доз(а)')):

                            # if debug: print(f"calc_doze_ls:  type(dosage_standard_value): {type(dosage_standard_value)}, type(vol): {type(vol)}")
                            if (((type(dosage_standard_value)==list) or (type(dosage_standard_value)==np.ndarray)) and len(dosage_standard_value)>0) \
                            or (((type(vol)==list) or (type(vol)==np.ndarray)) and len(vol)>0):
                                doze_by_doze_vol = value_empty
                            else:
                                try:
                                    doze_by_doze_vol = float(dosage_standard_value) * float(vol)
                                except Exception as err:
                                    # иногда встречается спсиок
                                    print (i_row, err)
                                    # print(vol, vol_unit, dosage_standard_value, dosage_standard_unit)
                                    doze_by_doze_vol = value_empty
                                    # sys.exit(2)

            #     else:
            #         doze_by_doze_vol = value_empty
            # else: 
            #     doze_by_doze_vol = value_empty
    if dosage_standard_unit is not None and (type(dosage_standard_unit)==str):
        doze_unit_by_doze_vol = re.sub(r"(/.*)", '', dosage_standard_unit).strip()
    else:
        doze_unit_by_doze_vol = dosage_standard_unit

    if debug: print("calc_doze_ls: doze_by_vol..., doze_unit_by_vol", doze_by_vol, doze_unit_by_vol, doze_by_doze, doze_unit_by_doze, \
          doze_by_doze_vol, doze_unit_by_doze_vol)
    
    if write:
        zvnlp_df.loc[i_row, update_cols_names] = np.array([doze_by_vol, doze_unit_by_vol, doze_by_doze, doze_unit_by_doze, \
              doze_by_doze_vol, doze_unit_by_doze_vol], dtype=object)
        if debug: 
            row_upd = zvnlp_df.loc[i_row, zvnlp_df.columns]
            print("proc_complex_doze: row_upd:")
            display(pd.DataFrame([row_upd]))

    return cnt

def apply_p6_calc_doze_ls():
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date  
    logger.info("P6: Расчет дозы ЛС - start ...")
    debug, write, b,e, cnt, max_cnt = False, True, 0, zvnlp_df.shape[0]+1, 0, zvnlp_df.shape[0]+1

    # debug, write, b,e, cnt, max_cnt = False, True, 0, 100, 0, 100
    # Wall time: 1.14 s
    # debug, write, b,e, cnt, max_cnt = False, True, 0, 1000, 0, 1000
    # Wall time: 5.61 s
    update_cols_names = ['расчет ЛС (по объему)', 'ЕИ ЛС (по объему)',  'расчет ЛС (по дозировке)', 'ЕИ ЛС (по дозировке)', 
                            'расчет ЛС (по дозировке/объему)', 'ЕИ ЛС (по дозировке/объему)']
    zvnlp_df[update_cols_names] = None
    err_cnt = 0
    err_i = []
    err_desc = []
    # for i, row in zvnlp_df[mask_dosage_standard_value_isnull].iterrows():
    for i, row in tqdm(zvnlp_df.iterrows(), total = zvnlp_df.shape[0]):
        if i < b: continue
        if i > e: break
        if cnt> max_cnt: break
        vol, vol_unit, dosage_standard_value, dosage_standard_unit = \
            row['vol'], row['vol_unit'], row['dosage_standard_value'], row['dosage_standard_unit']
        try:
            cnt = calc_doze_ls(i, vol, vol_unit, dosage_standard_value, dosage_standard_unit, 
                        cnt, debug=debug, write=write)
        except Exception as err:
            # print(i, err, '\n')
            err_cnt += 1
            err_i.append(i)
            err_desc.append(err)
            # print(vol, vol_unit, dosage_standard_value, dosage_standard_unit)
            # break
        # break
    # print("cnt:", cnt) # cnt: 33698
    logger.info("P6: Расчет дозы ЛС - done!")
    logger.info(f"Обработано записей: {cnt} ")
    print("err_cnt:", err_cnt) # err_cnt: 73
    print("err_i:", err_i)
    print("err_desc", list(set(err_desc)))
    
def is_value_contains_list(value):
    # содержит ли поле список из больше чем одного элемента 
    # пока просто по скобке без подсчет кол-ва элементов 
    # сплитить по разным условиям и считать - долго
    # но зато схватило ВАКЦИНА ДЛЯ ПРОФИЛАКТИКИ ГРИППА [ИНАКТИВИРОВАН...
    if value is None: return False
    # if ((type(value)==str) or (type(value)==np.str_)) and '[' in value: return True
    if ((type(value)==str) or (type(value)==np.str_)) and "' '" in value: return True
    # else: return False
    elif ((type(value)==list) or (type(value)==np.ndarray)) and value.shape[0]>1: return True
    else: return False
    return True
def determine_c_mnn(mnn_standard, form_standard, #form_standard_unify, is_narcotic, dosage_standard_value_str,
                               debug=False):
    c_mnn = 1
    for v in [mnn_standard, form_standard, ]: # form_standard_unify, is_narcotic, dosage_standard_value_str
        if is_value_contains_list(v):
            c_mnn = 2
            break
    return c_mnn

def determine_сontrollings(i_row, proc_tag, proc_tag2, doze_group, dosage_standard_value, vol,
                           mnn_standard, form_standard, form_standard_unify, is_narcotic, dosage_standard_value_str,
                           cnt, debug=False, write=False):
    update_cols_names = ['c_mnn', 'c_doze', 'c_vol','c_total',]
    c_mnn, c_doze, c_vol, c_total = None, None, None, None
    # c_mnn
    cnt += 1
    if (proc_tag is not None and ((type(proc_tag)==str) or (type(proc_tag)==np.str_)) and (\
        (('lp_date' in proc_tag) or ('lp_no_date' in proc_tag))))\
        or (proc_tag2 is not None and ((type(proc_tag2)==str) or (type(proc_tag2)==np.str_)) and \
            (('ph_form, doze, pack' in proc_tag2) and ('tn_mnn' in proc_tag2) )):  # ('ath' in proc_tag2) лишнее
      # (('ph_form, doze, pack+tn_mnn' in proc_tag2) or ('pack+tn_mnn, ph_form, doze, pack+tn_mnn+ath' in proc_tag2))\
    # если есть метки = lp_date, lp_no_date, not_matched = ph_form, doze, pack+tn_mnn, ph_form, doze, pack+tn_mnn+ath => = 1
    # иначе 0
        c_mnn = determine_c_mnn(mnn_standard, form_standard, #form_standard_unify, is_narcotic, dosage_standard_value_str,
                               debug=debug)
        # c_mnn = 1
    else:
        c_mnn = 0
        # 0 - пусто + tn_mnn + ph_form, doze, pack # это все остальное
        # 0 - пусто или tn_mnn или ph_form, doze, pack # переводим '+' в И или ИЛИ

    # c_doze
    # Если dosage_standard_value - содержит "{" => = 2 (2 - это требует обработки)
    # Если dosage_standard_value - #НД => = 2 (2 - это требует обработки)
    # иначе = 1
    # Как надо: если поле "dosage_standard_value" - заполнено => 1; не заполнено => 0; если имеет квадратные скобки => 2
    # if dosage_standard_value is not None:
    if dosage_standard_value is not None and \
        not (((type(dosage_standard_value)==float) or (type(dosage_standard_value)==np.float64)) and math.isnan(dosage_standard_value)):
        if ((((type(dosage_standard_value)==str) or (type(dosage_standard_value)==np.str_)) \
         and ('[' in dosage_standard_value or '#НД' in dosage_standard_value)) or\
        (not ((type(dosage_standard_value)==str) or (type(dosage_standard_value)==np.str_)) and\
         ((type(dosage_standard_value)==list) or (type(dosage_standard_value)==np.ndarray)) \
          and len(dosage_standard_value)>1)\
        ):
            c_doze = 2
        else:
            c_doze = 1
    else:
        c_doze = 0
    # c_vol
    # Если vol - содержит "[" => = 2
    # Если vol = #НД => = 2
    # Если vol = ** => = 1
    # Как надо: vol - заполнено => 1, не заполнено =>0 (сейчас не отрабатывает эту часть); содержит - #НД, [] =>2
    if vol is not None and not (((type(vol)==float) or (type(vol)==np.float64)) and math.isnan(vol)):
        if ( (((type(vol)==str) or (type(vol)==np.str_)) and ('[' in vol or ('#НД' in vol))) or\
            ((not (type(vol)==str) and ((type(vol)==list) or type(vol)==np.ndarray)) \
                  and len(vol)>1)\
          ):
            c_vol = 2
        # elif ((type(vol)==str) or (type(vol)==np.str_)) and ('**' in vol) and doze_group in [0,6,9]:
        #     c_vol = 1
        else:
            c_vol = 1
    # elif ((((type(vol)==str) or (type(vol)==np.str_)) and not ('#НД' in vol)) \
    #           or ((type(vol)==float) and not (math.isnan(vol))) ): #\
    #         # and not (doze_group in [0,6,9]):
    #         c_vol = 1
    else: 
        c_vol = 0

    if debug:
        print(f"determine_сontrollings: {i_row}, proc_tag: {proc_tag}, proc_tag2: {proc_tag2}, c_mnn: {c_mnn}, c_doze: {c_doze}, c_vol: {c_vol}, c_total: {c_total}")
        print(f"determine_сontrollings: {i_row}, dosage_standard_value: {dosage_standard_value}, vol: {vol}")

    if write:
        zvnlp_df.loc[i_row, update_cols_names] = c_mnn, c_doze, c_vol, c_total
        if debug: 
            row_upd = zvnlp_df.loc[i_row, zvnlp_df.columns]
            print("determine_сontrollings: row_upd:")
            display(pd.DataFrame([row_upd]))

    return cnt

def apply_p7_calc_controllings():
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date  
    logger.info("P7: Расчет контроллингов - start ...")
    debug, write, b,e, cnt, max_cnt = False, True, 0, zvnlp_df.shape[0]+1, 0, zvnlp_df.shape[0]+1
    # debug, write, b,e, cnt, max_cnt = True, False, 0, 30, 0, 2

    update_cols_names = ['c_mnn', 'c_doze', 'c_vol','c_total',]
    zvnlp_df[update_cols_names] = None

    # for i, row in zvnlp_df[mask_dosage_standard_value_isnull].iterrows():
    for i, row in tqdm(zvnlp_df.iterrows(), total= zvnlp_df.shape[0]):
        if i < b: continue
        if i > e: break
        if cnt> max_cnt: break
        proc_tag, proc_tag2, doze_group, dosage_standard_value, vol,\
        mnn_standard, form_standard, form_standard_unify, is_narcotic, dosage_standard_value_str  = \
            row['proc_tag'], row['proc_tag2'], row['doze_group'], row['dosage_standard_value'], row['vol'],\
            row['mnn_standard'], row['form_standard'], row['form_standard_unify'], row['is_narcotic'], row['dosage_standard_value_str']
            
        cnt = determine_сontrollings(i, proc_tag, proc_tag2, doze_group, dosage_standard_value, vol,
                mnn_standard, form_standard, form_standard_unify, is_narcotic, dosage_standard_value_str,
                cnt, debug=debug, write=write)
        # break
    # print(cnt) # 
    logger.info("P7: Расчет контроллингов - done!")
    logger.info(f"Обработано записей: {cnt} ")

def stat_controllins():
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date  
    print("c_mnn==0:", zvnlp_df[ (zvnlp_df['c_mnn'].notnull()) & (zvnlp_df['c_mnn']==0)].shape[0]) # 
    print("c_mnn==1:", zvnlp_df[ (zvnlp_df['c_mnn'].notnull()) & (zvnlp_df['c_mnn']==1)].shape[0]) # 
    print("c_mnn==2:", zvnlp_df[ (zvnlp_df['c_mnn'].notnull()) & (zvnlp_df['c_mnn']==2)].shape[0]) # 
    print("c_doze==0:", zvnlp_df[ (zvnlp_df['c_doze'].notnull()) & (zvnlp_df['c_doze']==0)].shape[0]) # 
    print("c_doze==1:", zvnlp_df[ (zvnlp_df['c_doze'].notnull()) & (zvnlp_df['c_doze']==1)].shape[0]) # 
    print("c_doze==2:", zvnlp_df[ (zvnlp_df['c_doze'].notnull()) & (zvnlp_df['c_doze']==2)].shape[0]) # 
    print("c_vol==0:", zvnlp_df[ (zvnlp_df['c_vol'].notnull()) & (zvnlp_df['c_vol']==0)].shape[0]) # 
    print("c_vol==1:", zvnlp_df[ (zvnlp_df['c_vol'].notnull()) & (zvnlp_df['c_vol']==1)].shape[0]) # 
    print("c_vol==2:", zvnlp_df[ (zvnlp_df['c_vol'].notnull()) & (zvnlp_df['c_vol']==2)].shape[0]) #    

def stat_snvlp (df_analysis):
    global smnn_list_df, klp_list_dict_df, zvnlp_df
    parsing_stat = {}
    if 'name' in df_analysis.attrs: parsing_stat['name'] = df_analysis.attrs['name']
    else: parsing_stat['name'] = None
    if 'esklp' in df_analysis.attrs: parsing_stat['esklp'] = df_analysis.attrs['esklp']
    else: parsing_stat['esklp'] = None
    if 'datetime_stamp' in df_analysis.attrs: parsing_stat['datetime_stamp'] = df_analysis.attrs['datetime_stamp']
    else: parsing_stat['datetime_stamp'] = None
    if 'calc_time' in df_analysis.attrs: parsing_stat['calc_time'] = df_analysis.attrs['calc_time']
    else: parsing_stat['calc_time'] = None

    p_name = f"Дата реестра:"
    print(p_name, znvlp_date)
    parsing_stat[p_name] = znvlp_date
    p_name = f"Всего позиций:"
    print(p_name, f"{df_analysis.shape[0]}")
    parsing_stat[p_name] = df_analysis.shape[0]

    p_name = f"Полная унификация:"
    values = df_analysis[df_analysis['n_code_klp']>0].shape[0]
    is_full_name, num = True, 1
    print(p_name if is_full_name else p_name.split('\n')[-1],  f"{values}")
    parsing_stat[p_name] = [num, is_full_name, values]
    
    p_name = "Полная унификация:\n" + \
        f"-- Из них по связке: (Дата регистрации цены; Штрих-код):"
    values = df_analysis[df_analysis['proc_tag']=='lp_date'].shape[0]
    is_full_name, num = False, num+1
    print(p_name if is_full_name else p_name.split('\n')[-1],  f"{values}")
    parsing_stat[p_name] = [num, is_full_name, values]
    
    p_name = f"Полная унификация:\n" +\
          f"-- Из них по связке: (Дата регистрации цены; Штрих-код):\n" +\
          f"--- в т.ч. один-к-одному с записью ЕСКЛП :"
    values = df_analysis[(df_analysis['proc_tag']=='lp_date') & df_analysis['n_code_klp']==1].shape[0]
    is_full_name, num = False, num+1
    print(p_name if is_full_name else p_name.split('\n')[-1],  f"{values}")
    parsing_stat[p_name] = [num, is_full_name, values]

    p_name = f"Полная унификация:\n"+ \
        f"-- Из них только по Штрихкоду:"
    values = df_analysis[df_analysis['proc_tag']=='lp_no_date'].shape[0]
    is_full_name, num = False, num+1
    print(p_name if is_full_name else p_name.split('\n')[-1],  f"{values}")
    parsing_stat[p_name] = [num, is_full_name, values]

    p_name = f"Полная унификация:\n"+ f"-- Из них только по Штрихкоду:\n" +\
         f"--- в т.ч. один-к-одному с записью ЕСКЛП :"
    values = df_analysis[(df_analysis['proc_tag']=='lp_no_date') & (df_analysis['n_code_klp']==1)].shape[0]
    is_full_name, num = False, num+1
    print(p_name if is_full_name else p_name.split('\n')[-1],  f"{values}")
    parsing_stat[p_name] = [num, is_full_name, values]
                    
    p_name = "НЕ унифицировано по связке: (Дата регистрации цены; Штрих-код) или только по Штрих-коду:"
    values = df_analysis[df_analysis['n_code_klp']==0].shape[0]
    is_full_name, num = True, num+1
    print(p_name if is_full_name else p_name.split('\n')[-1],  f"{values}")
    parsing_stat[p_name] = [num, is_full_name, values]

    p_name = f"Частичная унификация по полю 'Лекарственная форма, дозировка, упаковка (полная)':\n" +\
          f"- всего уникальных:"
    values = df_analysis['Лекарственная форма, дозировка, упаковка (полная)'].nunique()
    is_full_name, num = True, num+1
    print(p_name if is_full_name else p_name.split('\n')[-1],  f"{values}")
    parsing_stat[p_name] = [num, is_full_name, values]

    p_name = f"Частичная унификация по полю 'Лекарственная форма, дозировка, упаковка (полная)':\n"+\
          f"--- Унифицировано позиций - поля ЛФ, дозировки, упаковки:"
    values = df_analysis[df_analysis['proc_tag2'].notnull() & df_analysis['proc_tag2'].str.contains('ph_form, doze, pack')].shape[0]
    #одинаково  zvnlp_df[zvnlp_df['n_code_klp']==0]
    is_full_name, num = False, num+1
    print(p_name if is_full_name else p_name.split('\n')[-1],  f"{values}")
    parsing_stat[p_name] = [num, is_full_name, values]
    
          
    p_name = "Частичная унификация по полям 'МНН' и 'Торговое наименование лекарственного препарата':\n"+\
          f"--- Унифицировано позиций - поля ТН, МНН:"
    values = df_analysis[df_analysis['proc_tag2'].notnull() & df_analysis['proc_tag2'].str.contains('tn_mnn')].shape[0]
      #одинаково  zvnlp_df[zvnlp_df['n_code_klp']==0]
    is_full_name, num = True, num+1
    print(p_name if is_full_name else p_name.split('\n')[-1],  f"{values}")
    parsing_stat[p_name] = [num, is_full_name, values]

    p_name = "Частичная унификация по полям 'МНН' и 'Лекарственная форма...':\n"+\
          f"--- Унифицировано позиций - поля АТХ (код, название), ЖНВЛП да/нет, Наркотич. ЛП да/нет):"
    values = df_analysis[df_analysis['proc_tag2'].notnull() & df_analysis['proc_tag2'].str.contains('ath')].shape[0]
    #одинаково  zvnlp_df[zvnlp_df['n_code_klp']==0]
    is_full_name, num = True, num+1
    print(p_name if is_full_name else p_name.split('\n')[-1],  f"{values}")
    parsing_stat[p_name] = [num, is_full_name, values]

    p_name = "Позиций с рассчитанными объемами по ЖНВЛП:"
    # values = df_analysis[df_analysis['vol'].notnull() & ~(df_analysis['vol']=='#НД')].shape[0]
    values = df_analysis[~(df_analysis['vol'].notnull() & (df_analysis['vol'].str.contains('#НД')))].shape[0]
    is_full_name, num = True, num+1
    print(p_name if is_full_name else p_name.split('\n')[-1],  f"{values}")
    parsing_stat[p_name] = [num, is_full_name, values]
    p_name = "Позиций с объемами из КЛП:"
    values = df_analysis[df_analysis['mass_volume_num'].notnull() ].shape[0]
    is_full_name, num = True, num+1
    print(p_name if is_full_name else p_name.split('\n')[-1],  f"{values}")
    parsing_stat[p_name] = [num, is_full_name, values]

    return parsing_stat

def parse_opt():
    parser = argparse.ArgumentParser()
    parser.add_argument('--xlsx_zip', '-z', type=str, default=None,
        help="File 'lpYYYY-MM-DD-1.zip' in dir 'D:/DPP/01_parsing/data/znvlp/source/'")
    parser.add_argument('--esklp_date', '-k', type=str, default='last',
        help="Date of ESKLP file in format 'DD.MM.YYYY' in dir 'D:/DPP/01_parsing/data/esklp/processed/'")
    # parser.add_argument('--smnn', '-s', type=str, default='last',
    #     help="File 'smnn_list_df_esklp_*.pickle' in dir 'D:/DPP/01_parsing/data/esklp/processed/'")
    # parser.add_argument('--klp', '-k', type=str, default='last',
    #     help="File 'klp_list_dict_df_esklp_*.pickle' in dir 'D:/DPP/01_parsing/data/esklp/processed/'")        
    parser.add_argument('--part', '-p', type=str, default='all',
        help="'Части для выполнения алгоритма' in dir 'D:/DPP/01_parsing/data/znvlp/temp/'")   
    parser.add_argument('--pickle_file', '-pf', type=str, default='last',
        help="File 'znvlp_YYYYYMMDD_esklp_YYYYMMDD_p*.pickle' in dir 'D:/DPP/01_parsing/data/znvlp/temp/'")        
    parser.add_argument('--excel_save', '-xl', type=bool, default=False,
        help="Необходимость сохранения в Excel 'znvlp_YYYYYMMDD_esklp_YYYYMMDD_p*.xlsx' in dir 'D:/DPP/01_parsing/data/znvlp/processed/'")          
    parser.add_argument('--mode', '-m', type=str, default='run',
        help="run/test'")      
    parser.add_argument('--beg_rec', '-b', type=int, default=0,
        help="Номер начальнйо записи выборки")
    parser.add_argument('--end_rec', '-e', type=int, default=np.inf,
        help="Номер конечной записи выборки")
    opt = parser.parse_args()
    return opt

# def main (xlsx_zip=None, 
def parse_znvlp (
    path_znvlp_source,  path_znvlp_processed,  path_znvlp_work,    path_esklp_processed,
    xlsx_zip=None, 
    esklp_date = 'last',
    # smnn='last', klp ='last', 
    part = 'all', pickle_file = 'last', excel_save = False, 
        beg_rec = 0, end_rec = np.inf,
        mode = 'run', debug=False
        ):
    global smnn_list_df, klp_list_dict_df, zvnlp_df, znvlp_date, znvlp_date_format, esklp_date_format #esklp_date
    # if part is not None and part in ['all', 'p1']:
    
    if xlsx_zip is None:
        logger.error('No source zip xlsx file name')
        sys.exit(2)
    elif not os.path.exists(os.path.join(path_znvlp_source,  xlsx_zip)):
        logger.error(f"Not found source zip file: '{xlsx_zip}' in '{path_znvlp_source}'")
        sys.exit(2)
    
    if esklp_date == 'last':
        fn_smnn_list_df_pickle = find_last_file(path_esklp_processed, smnn_prefix, pickle_suffix)
        # print(f"fn_smnn_list_df_pickle: '{fn_smnn_list_df_pickle}'")
        fn_klp_list_dict_df_pickle = find_last_file(path_esklp_processed, klp_prefix, pickle_suffix)
        smnn_date = exract_esklp_date (fn_smnn_list_df_pickle, smnn_prefix)
        print(f"smnn_date: {smnn_date}")
        if fn_smnn_list_df_pickle is None or fn_klp_list_dict_df_pickle is None:
            logger.error(f"Не найдены файлы ЕСКЛП: убедитесь, что выполпнили обработку ЕСКЛП xml.zip-файла")
        klp_date = exract_esklp_date (fn_klp_list_dict_df_pickle, klp_prefix)
        print(f"klp_date: {klp_date}")
        if smnn_date != klp_date:
            logger.error('Dates of smnn & klp files are differeте or files are not found')
            sys.exit(2)
        else: 
            esklp_date_format = smnn_date
    else: 
        try:
            esklp_date_format = ''.join(esklp_date.split('.')[::-1])
        except Exception as err:
            logger.error("Неправильный формат даты; " + esklp_date + err)
        fn_smnn_list_df_pickle = find_last_file(path_esklp_processed, smnn_prefix + '_' + esklp_date_format,  pickle_suffix)
        fn_klp_list_dict_df_pickle = find_last_file(path_esklp_processed, klp_prefix + '_' + esklp_date_format,  pickle_suffix)
    
    print(f"esklp_date_format: '{esklp_date_format}'")
    
    if fn_smnn_list_df_pickle is None or fn_klp_list_dict_df_pickle is None:
        logger.error(f"smnn &/| klp files with  date: '{esklp_date}' are not found in '{path_esklp_processed}'")
        sys.exit(2)

    
    b, e = beg_rec, None if end_rec==np.inf else end_rec
    if 'all' in part or 'p1' in part:
        if xlsx_zip is None or (xlsx_zip == 'last'):
            fn_list = sorted(glob.glob(path_znvlp_source + 'lp*.zip'))
            if len(fn_list) > 0:
                fn_znvlp_zip = fn_list[-1]
            else:
                # fn_znvlp_zip = 'lp2022-11-11-1.zip'
                fn_znvlp_zip = ''
        else:
            fn_znvlp_zip = xlsx_zip
        
        if not os.path.exists(os.path.join(path_znvlp_source, fn_znvlp_zip)):
            logging.error(f"File '{fn_znvlp_zip}' not exists in {path_znvlp_source} ")
            sys.exit(2)
        fn_znvlp_xlsx = unzip_file(path_znvlp_source, fn_znvlp_zip, path_znvlp_work)
        

        zvnlp_df, znvlp_date = read_znvlp(path_znvlp_work,fn_znvlp_xlsx, b=b, e=e)
        # print("znvlp:", zvnlp_df.shape, znvlp_date)
        # print(dict(zip(zvnlp_df.columns, zvnlp_df.values[0])))
        znvlp_date_format = ''.join(znvlp_date.split('.')[::-1])
        # print(znvlp_date_format)

    # if smnn == 'last':
    #     fn_smnn_list_df_pickle = find_last_fn_pickle(smnn_prefix, path_esklp_processed)
    # else: fn_smnn_list_df_pickle = smnn
    # esklp_date_smnn = exract_esklp_date (fn_smnn_list_df_pickle)
    # # print( get_humanize_filesize(path_esklp_processed, fn_smnn_list_df_pickle))

    # if klp == 'last':
    #     fn_klp_list_dict_df_pickle = find_last_fn_pickle(klp_prefix, path_esklp_processed)
    # else: fn_klp_list_dict_df_pickle = klp
    # # print( get_humanize_filesize(path_esklp_processed, fn_klp_list_dict_df_pickle))
    # esklp_date_klp = exract_esklp_date (fn_klp_list_dict_df_pickle)

    # if esklp_date_smnn is not None and esklp_date_klp is not None:
    #     if esklp_date_smnn == esklp_date_klp:
    #         esklp_date = esklp_date_klp
    #     else:
    #         logging.error(f"Разные даты файлов SMNN и KLP: '{esklp_date_smnn}' vs {esklp_date_klp} ")
    #         sys.exit(2)
    # else:
    #     logging.error(f"Нвозможно определить даты файлов SMNN и KLP: '{fn_smnn_list_df_pickle}' vs {fn_klp_list_dict_df_pickle} ")
    #     sys.exit(2)

    if (pickle_file == 'last') and part in ['p2', 'p3a', 'p3b', 'p3c', 'p4', 'p5', 'p6', 'p7' ]:
        # znvlp_date_format = ''.join(zvnlp_df.attrs['date'].split('.')[::-1])
        pickle_prefix = 'znvlp_' + znvlp_date_format + '_esklp_' + esklp_date_format + '_'
        pickle_fn = find_last_fn_pickle(pickle_prefix, path_znvlp_work)
        # print("pickle_fn: ", pickle_fn)
    else: 
        pickle_prefix = ''
        pickle_fn = pickle_file 
        pass
    # print("pickle_fn: ", pickle_fn)
    
    smnn_list_df = restore_df_from_pickle(smnn_prefix, 
        path_esklp_processed, fn_smnn_list_df_pickle)
    
    klp_list_dict_df = restore_df_from_pickle(klp_prefix, 
        path_esklp_processed, fn_klp_list_dict_df_pickle)
    
     
    init_extend_znvlp()
    # for i_row in [0, 1, 2, 5, 582, 583]:
    #     test_extend_znvlp(i_row)
    
    # numba.set_num_threads(int(numba.get_num_threads()/2))
    numba.set_num_threads(2)
    e = zvnlp_df.shape[0]
    if mode=='run':
        if part == 'all':
            apply_p1_lp_date()
            apply_p2_lp_no_date()
            apply_p3a_cnt_code_klp()
            apply_p3a_ph_form()
            apply_p3b_pre_clean_tn()
            apply_p3b_tn_mnn()
            apply_p3c_mnn_standard_form_standard()
            apply_p4_calc_volumes()
            apply_p5_calc_complex_doze(debug)
            apply_p6_calc_doze_ls()
            apply_p7_calc_controllings()
            stat_controllins()
            znvlp_parsing_stat = stat_snvlp(zvnlp_df)
            
            tmp_fn_main = 'znvlp_' + znvlp_date_format + '_esklp_' + esklp_date_format + '_all_steps'
        # elif part == 'p1':
        elif not set(part.split('+')).issubset(set(['p1', 'p2', 'p3a', 'p3b', 'p3c', 'p4', 'p5', 'p6', 'p7'])):
            logger.error(f"Не определены этапы расчетов")
            sys.exit(2)
        else:
            if 'p1' in part:
                # e = 1000
                # apply_p1_lp_date(e = e) # e = 1000
                
                # apply_p1_lp_date(b = 32800, e=33771) # e = 1000
                apply_p1_lp_date() 
                tmp_fn_main = 'znvlp_' + znvlp_date_format + '_esklp_' + esklp_date_format + '_p1'
                # print(zvnlp_df[zvnlp_df['proc_tag']=='lp_date'].shape) # 30479
                
            # elif part == 'p2':
            if 'p2' in part:
                if '+p2' in part: pass
                else:
                    zvnlp_df = restore_df_from_pickle(pickle_prefix + 'p1', path_znvlp_work, pickle_fn)
                # znvlp_date_format = ''.join(zvnlp_df.attrs['date'].split('.')[::-1])
                apply_p2_lp_no_date()
                tmp_fn_main = 'znvlp_' + znvlp_date_format + '_esklp_' + esklp_date_format + '_p2'
                # print(zvnlp_df[zvnlp_df['proc_tag']=='lp_no_date'].shape)
                
            # elif part == 'p3a':
            if 'p3a' in part:
                if '+p3a' in part: pass
                else:
                    zvnlp_df = restore_df_from_pickle(pickle_prefix + 'p2', path_znvlp_work, pickle_fn)
                # znvlp_date_format = ''.join(zvnlp_df.attrs['date'].split('.')[::-1])
                apply_p3a_cnt_code_klp()
                apply_p3a_ph_form()
                tmp_fn_main = 'znvlp_' + znvlp_date_format + '_esklp_' + esklp_date_format + '_p3a'
            # elif part == 'p3b':
            if 'p3b' in part:
                if '+p3b' in part: pass
                else:
                    zvnlp_df = restore_df_from_pickle(pickle_prefix + 'p3a', path_znvlp_work, pickle_fn)
                # znvlp_date_format = ''.join(zvnlp_df.attrs['date'].split('.')[::-1])
                apply_p3b_pre_clean_tn()
                apply_p3b_tn_mnn()
                tmp_fn_main = 'znvlp_' + znvlp_date_format + '_esklp_' + esklp_date_format + '_p3b'
            # elif part == 'p3c':
            if 'p3c' in part:
                if '+p3c' in part: pass
                else:
                    zvnlp_df = restore_df_from_pickle(pickle_prefix + 'p3b', path_znvlp_work, pickle_fn)
                # znvlp_date_format = ''.join(zvnlp_df.attrs['date'].split('.')[::-1])
                apply_p3c_mnn_standard_form_standard()
                tmp_fn_main = 'znvlp_' + znvlp_date_format + '_esklp_' + esklp_date_format + '_p3c'
            # elif part == 'p4':
            if 'p4' in part:
                if '+p4' in part: pass
                else:
                    zvnlp_df = restore_df_from_pickle(pickle_prefix + 'p3c', path_znvlp_work, pickle_fn)
                # znvlp_date_format = ''.join(zvnlp_df.attrs['date'].split('.')[::-1])
                apply_p4_calc_volumes()
                tmp_fn_main = 'znvlp_' + znvlp_date_format + '_esklp_' + esklp_date_format + '_p4'
            # elif part == 'p5':
            if 'p5' in part:
                if '+p5' in part: pass
                else:
                    zvnlp_df = restore_df_from_pickle(pickle_prefix + 'p4', path_znvlp_work, pickle_fn)        
                # znvlp_date_format = ''.join(zvnlp_df.attrs['date'].split('.')[::-1])
                apply_p5_calc_complex_doze()
                tmp_fn_main = 'znvlp_' + znvlp_date_format + '_esklp_' + esklp_date_format + '_p5'
            # elif part == 'p6':
            if 'p6' in part:
                if '+p6' in part: pass
                else:
                    zvnlp_df = restore_df_from_pickle(pickle_prefix + 'p5', path_znvlp_work, pickle_fn)  
                # znvlp_date_format = ''.join(zvnlp_df.attrs['date'].split('.')[::-1])
                apply_p6_calc_doze_ls()
                tmp_fn_main = 'znvlp_' + znvlp_date_format + '_esklp_' + esklp_date_format + '_p6'
            # elif part == 'p7':
            if 'p7' in part:
                if '+p7' in part: pass
                else:
                    zvnlp_df = restore_df_from_pickle(pickle_prefix + 'p6', path_znvlp_work, pickle_fn)  
                # znvlp_date_format = ''.join(zvnlp_df.attrs['date'].split('.')[::-1])
                apply_p7_calc_controllings()
                stat_controllins()
                znvlp_parsing_stat = stat_snvlp(zvnlp_df)
                tmp_fn_main = 'znvlp_' + znvlp_date_format + '_esklp_' + esklp_date_format + '_all_steps'
            
            
            tmp_fn_main_pickle = save_df_to_pickle(zvnlp_df, path_znvlp_work, tmp_fn_main)
        # if '_all_steps' in tmp_fn_main or excel_save:
        if 'all' in part or 'p7' in part or excel_save:
            for col in ['is_n_ru_in_ESKLP', 'is_n_ru_and_barcode_in_ESKLP']:
                if col in zvnlp_df.columns:
                    zvnlp_df.drop(columns = [col], inplace=True)
            
            tmp_fn_main_xlsx = save_df_to_excel(zvnlp_df, path_znvlp_processed, tmp_fn_main, e=e)

    else:
        i_row = 871
        test_extend_znvlp(i_row, is_reg_price=True, debug=False, debug_print=True, write=False)
    # tmp_1 = save_df_to_excel(smnn_list_df, path_esklp_processed, 'smnn_2022_11_10_server.xlsx')
    # tmp_1 = save_df_to_excel(klp_list_dict_df, path_esklp_processed, 'klp_2022_11_10_server.xlsx')

    #
    
    

    
    

# if __name__ == '__main__':
#     if len(sys.argv) > 1: # есть аргументы в командной строке
#         opt = parse_opt()
#         main(**vars(opt))
#     else:
#         main()
    
# Запуск по ЖНВЛП 19.10.2022 ЕСКЛП 23.09.2022
# py extend_znvlp_01.py -p all -z lp2022-11-23-1.zip -s smnn_list_df_esklp_20221123_2022_11_23_1728.pickle -k klp_list_dict_df_esklp_20221123_2022_11_23_1735.pickle
# py extend_znvlp_01.py -p all -z lp2022-12-26-1.zip -k 23.12.2022
