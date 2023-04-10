import ipywidgets as widgets
from ipywidgets import Layout, Box, Label
def form_znvlp_link():
    fn_znvlp_zip_file_drop_douwn = widgets.Text(value=None)
    form_item_layout = Layout(display='flex', flex_flow='row', justify_content='space-between')
    check_box = Box([Label(value="Введите скопрированную ссылку на excel.zip-файл c реестром ЖНВЛП:"), fn_znvlp_zip_file_drop_douwn], layout=form_item_layout) 
    form_items = [check_box] 
    
    form_znvlp = Box(form_items, layout=Layout(display='flex', flex_flow= 'column', border='solid 2px', align_items='stretch', width='70%')) #width='auto'))
    return form_znvlp, fn_znvlp_zip_file_drop_douwn
# form_znvlp, fn_znvlp_zip_file_drop_douwn =  form_znvlp_link()
# form_znvlp
