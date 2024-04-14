# read sqlite
# with leftjoin
import sqlite3
from re import sub
from translate.storage.tmx import tmxfile


conn = sqlite3.connect('norwegian.sqlite')
c = conn.cursor()
c.execute ("ATTACH DATABASE 'LUT.sqlite' AS german_bible;")
c.execute ("""
SELECT * from verse as nb
INNER JOIN german_bible.verse as de
ON nb.book_id == de.book_id 
AND nb.chapter == de.chapter
AND nb.verse == de.verse
"""
)

all_rows = c.fetchall()

SOURCE_LANG = "nb-NO"
TARGET_LANG = "de-DE"

tmxfile = tmxfile(None, SOURCE_LANG, TARGET_LANG)

for row in all_rows:
    
    src_line : str = row[4]
    tgt_line : str = row[9]
    
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
    
    #remove double space - is due to the previous statement, it will produce doulbe spaces in the text
    src_line = src_line.replace("  ", " ")
    tgt_line = tgt_line.replace("  ", " ")
    
    src_line = src_line.strip()
    tgt_line = tgt_line.strip()
    
    tmxfile.addtranslation(src_line, SOURCE_LANG, tgt_line, TARGET_LANG)
with open("de_bible_prototype.tmx", "wb") as output:
    tmxfile.serialize(output)