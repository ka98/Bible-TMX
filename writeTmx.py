# read sqlite
# with leftjoin
import sqlite3
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
    src_line = row[4].strip()
    tgt_line = row[9].strip()
    tmxfile.addtranslation(src_line, SOURCE_LANG, tgt_line, TARGET_LANG)
with open("de_bible_prototype.tmx", "wb") as output:
    tmxfile.serialize(output)