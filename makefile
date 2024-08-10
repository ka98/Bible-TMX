.PHONY: clean

clean:
	find . -name "*.tmx" -type f -delete 
	find . -name "*.xlsx" -type f -delete 
	# remove all directorys in the languages Folders
	rm -R ./res/*-*/*/