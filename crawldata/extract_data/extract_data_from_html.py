from extract_data import extract_text_from_html as efth
from lxml import html

def extract(html_str):
    text, dict_text = efth.extract_text(html_str, guess_layout=True)
    tree = html.fromstring(html_str)
    link_img_list = tree.xpath('//img/@src')
    return text, dict_text, link_img_list