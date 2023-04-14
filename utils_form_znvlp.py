import re
import ipywidgets as widgets
from ipywidgets import Layout, Box, Label
def form_znvlp_link():
    fn_znvlp_zip_file_text = widgets.Text(value=None)
    form_item_layout = Layout(display='flex', flex_flow='row', justify_content='space-between')
    check_box = Box([Label(value="Введите скопрированную ссылку на excel.zip-файл c реестром ЖНВЛП:"), fn_znvlp_zip_file_text], layout=form_item_layout) 
    form_items = [check_box] 
    
    form_znvlp = Box(form_items, layout=Layout(display='flex', flex_flow= 'column', border='solid 2px', align_items='stretch', width='70%')) #width='auto'))
    return form_znvlp, fn_znvlp_zip_file_text
   
# form_znvlp, fn_znvlp_zip_file_drop_douwn =  form_znvlp_link()
# form_znvlp
def form_esklp_dates(fn_list):
    esklp_dates = [re.findall(r'(?:\d\d\d\d\d\d\d\d)', fn) for fn in fn_list]
    esklp_dates = list(set([d[0] for d in esklp_dates if len(d) > 0]))
    return esklp_dates
def parapm_form_znvlp_esklp_dicts(fn_list):
    esklp_dates = form_esklp_dates(fn_list)
    esklp_dates_dropdown = widgets.Dropdown( options=esklp_dates) #, value=None)
    
    form_item_layout = Layout(display='flex', flex_flow='row', justify_content='space-between')
    check_box = Box([Label(value="Выберите дату ЕСКЛП справочника для использования:"), esklp_dates_dropdown], layout=form_item_layout) 
    form_items = [check_box]
    
    form_znvlp_esklp_dicts = Box(form_items, layout=Layout(display='flex', flex_flow= 'column', border='solid 2px', align_items='stretch', width='50%')) #width='auto'))
    # return form, fn_check_file_drop_douwn, fn_dict_file_drop_douwn, radio_btn_big_dict, radio_btn_prod_options, similarity_threshold_slider, max_entries_slider
    return form_znvlp_esklp_dicts, esklp_dates_dropdown 
