import re
import lxml
import lxml.etree
from lxml.html.clean import Cleaner

NEWLINE_TAGS = frozenset([
    'article', 'aside', 'br', 'dd', 'details', 'div', 'dt', 'fieldset',
    'figcaption', 'footer', 'form', 'header', 'hr', 'legend', 'li', 'main',
    'nav', 'table', 'tr'
])

DOUBLE_NEWLINE_TAGS = frozenset([
    'blockquote', 'dl', 'figure', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ol',
    'p', 'pre', 'title', 'ul'
])

cleaner = Cleaner(
    scripts=True,
    javascript=False,  
    comments=True,
    style=True,
    links=True,
    meta=True,
    page_structure=False,  
    processing_instructions=True,
    embedded=True,
    frames=True,
    forms=False, 
    annoying_tags=False,
    remove_unknown_tags=False,
    safe_attrs_only=False,
)


def _cleaned_html_tree(html):
    if isinstance(html, lxml.html.HtmlElement):
        tree = html
    else:
        tree = parse_html(html)
    try:
        cleaned = cleaner.clean_html(tree)
    except AssertionError:
        cleaned = tree
    return cleaned


def parse_html(html):
    body = html.strip().replace('\x00', '').encode('utf8') or b'<html/>'
    parser = lxml.html.HTMLParser(recover=True, encoding='utf8')
    root = lxml.etree.fromstring(body, parser=parser)
    if root is None:
        root = lxml.etree.fromstring(b'<html/>', parser=parser)
    return root


_whitespace = re.compile(r'\s+')
_has_trailing_whitespace = re.compile(r'\s$').search
_has_punct_after = re.compile(r'^[,:;.!?")]').search
_has_open_bracket_before = re.compile(r'\($').search


def _normalize_whitespace(text):
    return _whitespace.sub(' ', text.strip())


def etree_to_text(tree,
                  guess_punct_space=True,
                  guess_layout=True,
                  newline_tags=NEWLINE_TAGS,
                  double_newline_tags=DOUBLE_NEWLINE_TAGS):
    chunks = []
    chunks_with_xpath = []

    _NEWLINE = object()
    _DOUBLE_NEWLINE = object()

    class Context:
        prev = _DOUBLE_NEWLINE

    def should_add_space(text, prev):
        if prev in {_NEWLINE, _DOUBLE_NEWLINE}:
            return False
        if not guess_punct_space:
            return True
        if not _has_trailing_whitespace(prev):
            if _has_punct_after(text) or _has_open_bracket_before(prev):
                return False
        return True

    def get_space_between(text, prev):
        if not text:
            return ' '
        return ' ' if should_add_space(text, prev) else ''
    
    def add_newlines(tag, context):
        if not guess_layout:
            return
        prev = context.prev
        if prev is _DOUBLE_NEWLINE:  
            return
        if tag in double_newline_tags:
            context.prev = _DOUBLE_NEWLINE
            chunks.append('\n' if prev is _NEWLINE else '\n\n')
            chunks_with_xpath.append(dict(
                content='\n' if prev is _NEWLINE else '\n\n',
                xpath=None
            ))
        elif tag in newline_tags:
            context.prev = _NEWLINE
            if prev is not _NEWLINE:
                chunks.append('\n')
                chunks_with_xpath.append(dict(
                    content='\n',
                    xpath=None
                ))

    def add_text(root_tree, tree, text_content, context):
        text = _normalize_whitespace(text_content) if text_content else ''
        if not text:
            return
        space = get_space_between(text, context.prev)
        chunks.extend([space, text])
        chunks_with_xpath.extend([
            dict(
                content=space,
                xpath=None
            ),
            dict(
                content=text,
                xpath=root_tree.getpath(tree)
            )
        ])
        context.prev = text_content

    root_tree = tree.getroottree() 
    
    def traverse_text_fragments(root_tree, tree, context, handle_tail=True):
        add_newlines(tree.tag, context)
        add_text(root_tree, tree, tree.text, context)
        for child in tree:
            traverse_text_fragments(root_tree, child, context)
        add_newlines(tree.tag, context)
        if handle_tail:
            add_text(root_tree, tree, tree.tail, context)

    traverse_text_fragments(root_tree, tree, context=Context(), handle_tail=False)
    # delete the empty xpaths
    dict_xpath = {}
    for e in chunks_with_xpath:
        if e['xpath'] != None:
            dict_xpath[e['xpath']] = e['content']
    
    return ''.join(chunks).strip(), dict_xpath

def selector_to_text(sel, guess_punct_space=True, guess_layout=True):
    import parsel
    if isinstance(sel, parsel.SelectorList):
        text = []
        for s in sel:
            extracted = etree_to_text(
                s.root,
                guess_punct_space=guess_punct_space,
                guess_layout=guess_layout)
            if extracted:
                text.append(extracted)
        return ' '.join(text)
    else:
        return etree_to_text(
            sel.root,
            guess_punct_space=guess_punct_space,
            guess_layout=guess_layout)


def cleaned_selector(html):
    import parsel
    try:
        tree = _cleaned_html_tree(html)
        sel = parsel.Selector(root=tree, type='html')
    except (lxml.etree.XMLSyntaxError,
            lxml.etree.ParseError,
            lxml.etree.ParserError,
            UnicodeEncodeError):
        sel = parsel.Selector(html)
    return sel


def extract_text(html,
                 guess_punct_space=True,
                 guess_layout=True,
                 newline_tags=NEWLINE_TAGS,
                 double_newline_tags=DOUBLE_NEWLINE_TAGS):
    if html is None:
        return ''
    cleaned = _cleaned_html_tree(html)
    return etree_to_text(
        cleaned,
        guess_punct_space=guess_punct_space,
        guess_layout=guess_layout,
        newline_tags=newline_tags,
        double_newline_tags=double_newline_tags,
    )