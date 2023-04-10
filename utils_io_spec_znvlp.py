import urllib.request
import zipfile
import sys, os
from utils_io_znvlp import logger, get_humanize_filesize

if len(logger.handlers) > 1:
    for handler in logger.handlers:
        logger.removeHandler(handler)
    from utils_io_znvlp import logger
def download_znvlp(data_znvlp_source_dir, znvlp_link):
    fn_znvlp = None
    print(f"znvlp_link: '{znvlp_link}'")
    if znvlp_link is None or (znvlp_link is not None and len(znvlp_link)==0):
        logger.error ("Повторите ввод ссылки в форму - сейчас она пуста")
        sys.exit(2)
    try:
        filedata = urllib.request.urlopen(znvlp_link) 
        datatowrite = filedata.read() 
        with open(os.path.join(data_znvlp_source_dir, 'lp.zip'), 'wb') as f: 
            f.write(datatowrite)
            fs = get_humanize_filesize(data_znvlp_source_dir, 'lp.zip')
            try:
                with zipfile.ZipFile(os.path.join(data_znvlp_source_dir, 'lp.zip'), 'r') as zip_ref:
                    fn_list = zip_ref.namelist()
                    if len(fn_list)>0:
                        fn_znvlp = fn_list[0]
                        zip_ref.extractall(data_znvlp_source_dir)
                        logger.info(f"'Unzip 'lp.zip' {fs} -> '{data_znvlp_source_dir}/{fn_znvlp}' - done!'")   
            except Exception as err:
                logger.error(str(err))
                logger.error(f"Ошибка скачанного файла - проверьте правильность скопированной ссылки и введите ее повторно в форму!")   
    except urllib.request.URLError as err:
        logger.error("Ошибка скачивания - проверьте правильность скопированной ссылки и введите ее повторно в форму!")
        try:         sys.exit(2)
        except:  pass

    return fn_znvlp
# znvlp_link = fn_znvlp_zip_file_drop_douwn.value
# fn_znvlp = download_znvlp(znvlp_link)
