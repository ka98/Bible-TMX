# read sqlite
# with leftjoin
import sqlite3
from lxml import etree
from translate.storage.tmx import tmxfile, tmxunit
from translate.misc.xml_helpers import setXMLlang
from pandas import DataFrame
from multiprocessing import Pool

from os.path import basename
import os

import input

# Patching TMX Creation Plugin
def addtranslation_patch(self, source, srclang, translation, translang, comment=None, context_key=None):
        """Addtranslation method for testing old unit tests."""
        unit = self.addsourceunit(source)
        unit.target = translation
        if comment is not None and len(comment) > 0:
            unit.addnote(comment)
            
        if context_key is not None and len(context_key) > 0:
            unit.addcontextkey(context_key)

        tuvs = unit.xmlelement.iterdescendants(self.namespaced("tuv"))
        setXMLlang(next(tuvs), srclang)
        setXMLlang(next(tuvs), translang)
        
def addcontextkey_patch(self, text, origin=None, position="append"):
        """
        Add a context_key property.

        The origin parameter is ignored
        """
        context_key = etree.SubElement(self.xmlelement, self.namespaced("prop"), {"type": "x-context_seg_key"})
        context_key.text = text.strip()
        
setattr(tmxfile, "addtranslation", addtranslation_patch)
setattr(tmxunit, "addcontextkey", addcontextkey_patch)
    
def align_bible(target):
    conn = sqlite3.connect(input.source)
    c = conn.cursor()
    c.execute (f"ATTACH DATABASE '{target}' AS target_bible;")
    
    # Spanish has some really wierd chapters... why???
    if target == "res/es-ES/es-ES_RVR1960.sqlite":
        c.execute ("""
        SELECT * from verse as nb
        INNER JOIN target_bible.book as book
        ON book.book_reference_id == tg.book_id
        INNER JOIN target_bible.mapping AS mapping
        ON nb.book_id == mapping.book_nb
        INNER JOIN target_bible.verse as tg
        ON mapping.book_es == tg.book_id
        AND nb.chapter == tg.chapter
        AND nb.verse == tg.verse
        """
        )
    
    #remove appocrypes have from Slovenian bible
    elif target == "res/sl-SI/sl-SI_SSP.sqlite":
        c.execute ("""
        SELECT * from verse as nb
        INNER JOIN target_bible.book as book
        ON book.book_reference_id == tg.book_id
        INNER JOIN target_bible.verse as tg
        ON ((book.testament_reference_id == 1 AND nb.book_id == tg.book_id) OR (book.testament_reference_id == 2 AND nb.book_id == (tg.book_id-10)))
        AND nb.chapter == tg.chapter
        AND nb.verse == tg.verse
        """
        )
    
    else:    
        c.execute ("""
        SELECT * from verse as nb
        INNER JOIN target_bible.book as book
        ON book.book_reference_id == tg.book_id
        INNER JOIN target_bible.verse as tg
        ON nb.book_id == tg.book_id
        AND nb.chapter == tg.chapter
        AND nb.verse == tg.verse
        """
        )

    all_rows = c.fetchall()
    
    SOURCE_LANG = basename(input.source)[:5]
    TARGET_LANG = basename(target)[:5]

    my_tmxfile = tmxfile(None, SOURCE_LANG, TARGET_LANG)

    for row in all_rows:
        
        src_line : str = row[4]
        if target == "res/es-ES/es-ES_RVR1960.sqlite":
            tgt_line : str = row[15]
        else:
            tgt_line : str = row[13]
        
        #remove weird closing bracket in norwegian 1930
        tgt_line = tgt_line.replace(":)", ")")
        src_line = src_line.replace(":)", ")")
        
        #remove (-) sign
        src_line = src_line.replace("(-)", " ")
        tgt_line = tgt_line.replace("(-)", " ")
        
        src_line = src_line.replace(":", ": ")
        tgt_line = tgt_line.replace(":", ": ")
        
        src_line = src_line.replace(",", ", ")
        tgt_line = tgt_line.replace(",", ", ")
        
        src_line = src_line.replace(".", ". ")
        tgt_line = tgt_line.replace(".", ". ")
        
        src_line = src_line.replace(";", "; ")
        tgt_line = tgt_line.replace(";", "; ")
        
        src_line = src_line.replace("!", "! ")
        tgt_line = tgt_line.replace("!", "! ")
        
        src_line = src_line.replace("?", "? ")
        tgt_line = tgt_line.replace("?", "? ")
        
        #remove double space - is due to the previous statement, it will produce doulbe spaces in the text
        src_line = src_line.replace("  ", " ")
        tgt_line = tgt_line.replace("  ", " ")
        
        src_line = src_line.strip()
        tgt_line = tgt_line.strip()
        
        my_tmxfile.addtranslation(src_line, SOURCE_LANG, tgt_line, TARGET_LANG, context_key=f"{row[8]} {row[2]},{row[3]}")
    with open(f"{target}NB88_07.tmx", "wb") as output:
        my_tmxfile.serialize(output)
    
    if target == "res/sl-SI/sl-SI_SSP.sqlite":
        c.execute ("""
            SELECT book.name, nb.chapter, nb.verse, nb.text FROM verse AS nb
    LEFT JOIN book AS book
    ON book.book_reference_id == nb.book_id
    LEFT JOIN target_bible.verse AS tg
    ON ((book.testament_reference_id == 1 AND nb.book_id == tg.book_id) OR (book.testament_reference_id == 2 AND nb.book_id == (tg.book_id-10)))
    AND nb.chapter == tg.chapter
    AND nb.verse == tg.verse
    WHERE tg.verse IS NULL
    UNION ALL
    SELECT book.name, tg.chapter, tg.verse, tg.text FROM target_bible.verse AS tg
    LEFT JOIN book AS book
    ON ((book.testament_reference_id == 1 AND book.book_reference_id == tg.book_id) OR (book.testament_reference_id == 2 AND book.book_reference_id == (tg.book_id-10)))
    LEFT JOIN verse AS nb
    ON ((book.testament_reference_id == 1 AND nb.book_id == tg.book_id) OR (book.testament_reference_id == 2 AND nb.book_id == (tg.book_id-10)))
    AND nb.chapter == tg.chapter
    AND nb.verse == tg.verse
    WHERE nb.verse IS NULL;
            """
            )
    
    else:
        c.execute ("""
            SELECT book.name, nb.chapter, nb.verse, nb.text FROM verse AS nb
    LEFT JOIN book AS book
    ON book.book_reference_id == nb.book_id
    LEFT JOIN target_bible.verse AS tg
    ON nb.book_id == tg.book_id
    AND nb.chapter == tg.chapter
    AND nb.verse == tg.verse
    WHERE tg.verse IS NULL
    UNION ALL
    SELECT book.name, tg.chapter, tg.verse, tg.text FROM target_bible.verse AS tg
    LEFT JOIN book AS book
    ON book.book_reference_id == tg.book_id
    LEFT JOIN verse AS nb
    ON nb.book_id == tg.book_id
    AND nb.chapter == tg.chapter
    AND nb.verse == tg.verse
    WHERE nb.verse IS NULL;
            """
            )
    
    all_rows = c.fetchall()
    df = DataFrame(all_rows, columns=['name', 'chapter', 'verse', 'text'])
    df.to_excel(f"{target}.xlsx", sheet_name='sheet1', index=False)
    
# multiprocessing
pool = Pool(os.cpu_count())    
pool.map(align_bible, input.targets)