# read sqlite
# with leftjoin
import sqlite3
from lxml import etree
from translate.storage.tmx import tmxfile, tmxunit
from translate.misc.xml_helpers import setXMLlang, valid_chars_only


conn = sqlite3.connect('nb-NO_DNB1930.sqlite')
c = conn.cursor()
c.execute ("ATTACH DATABASE 'nl-NL_HSV.sqlite' AS target_bible;")
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

# sl-SI ON ((book.testament_reference_id == 1 AND nb.book_id == tg.book_id) OR (book.testament_reference_id == 2 AND nb.book_id == (tg.book_id-10)))
# since appocrypes have to be removed...

#adding context_key functionality to TMX as described in https://support.phrase.com/hc/en-us/articles/9386879926556-Translation-Unit-Metadata#UUID-e959aca6-d39e-3521-412d-9c6087cdd346

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
        
#patch addtranslation

setattr(tmxfile, "addtranslation", addtranslation_patch)
setattr(tmxunit, "addcontextkey", addcontextkey_patch)

all_rows = c.fetchall()

SOURCE_LANG = "nb-NO"
TARGET_LANG = "nl-NL"

tmxfile = tmxfile(None, SOURCE_LANG, TARGET_LANG)

for row in all_rows:
    
    src_line : str = row[4]
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
    
    tmxfile.addtranslation(src_line, SOURCE_LANG, tgt_line, TARGET_LANG, context_key=f"{row[8]} {row[2]},{row[3]}")
with open("nl-NL_bible.tmx", "wb") as output:
    tmxfile.serialize(output)